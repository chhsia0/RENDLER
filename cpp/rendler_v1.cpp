/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <libgen.h>
#include <iostream>
#include <map>
#include <queue>
#include <string>
#include <vector>

#include <glog/logging.h>

#include <mesos/v1/mesos.hpp>
#include <mesos/v1/resources.hpp>
#include <mesos/v1/scheduler.hpp>

#include <process/delay.hpp>
#include <process/owned.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>

#include <stout/foreach.hpp>
#include <stout/os.hpp>

#include "rendler_helper.hpp"

using namespace mesos::v1;

using std::cout;
using std::endl;
using std::string;
using std::vector;
using std::queue;
using std::map;

using mesos::stringToVector;

using mesos::v1::scheduler::Call;
using mesos::v1::scheduler::Event;

const float CPUS_PER_TASK = 0.2;
const int32_t MEM_PER_TASK = 32;

static queue<string> crawlQueue;
static queue<string> renderQueue;
static map<string, vector<string> > crawlResults;
static map<string, string> renderResults;
static map<string, size_t> processed;
static size_t nextUrlId = 0;
static process::Owned<process::ProcessBase> rendler;

static void shutdown();
static void SIGINTHandler(int signum);

class RendlerV1 : public process::Process<RendlerV1>
{
public:
  RendlerV1(const FrameworkInfo& _framework,
            const ExecutorInfo& _crawler,
            const ExecutorInfo& _renderer,
            const string& _master,
            const string& _seedUrl)
    : framework(_framework),
      crawler(_crawler),
      renderer(_renderer),
      master(_master),
      state(INITIALIZING),
      seedUrl(_seedUrl),
      tasksLaunched(0),
      tasksFinished(0),
      frameworkMessagesReceived(0)
  {
    crawlQueue.push(seedUrl);
    renderQueue.push(seedUrl);
    processed[seedUrl] = nextUrlId++;
    size_t scheme = seedUrl.find_first_of("://");
    size_t lsp = seedUrl.find_last_of(
        '/', scheme == string::npos ? 0 : scheme + 3); // skip the http:// part
    baseUrl = seedUrl.substr(0, lsp); // No trailing slash
  }

  virtual ~RendlerV1() {}

  void connected()
  {
    state = CONNECTED;
    doReliableRegistration();
  }

  void disconnected()
  {
    state = DISCONNECTED;
  }

  void received(queue<Event> events)
  {
    while (!events.empty()) {
      Event event = events.front();
      events.pop();

      switch (event.type()) {
        case Event::SUBSCRIBED: {
          framework.mutable_id()->CopyFrom(event.subscribed().framework_id());
          state = SUBSCRIBED;

          cout << "Subscribed with ID " << framework.id() << endl;
          break;
        }

        case Event::OFFERS: {
          resourceOffers(google::protobuf::convert(event.offers().offers()));
          break;
        }

        case Event::INVERSE_OFFERS: {
          break;
        }

        case Event::RESCIND: {
          break;
        }

        case Event::RESCIND_INVERSE_OFFER: {
          break;
        }

        case Event::UPDATE: {
          statusUpdate(event.update().status());
          break;
        }

        case Event::MESSAGE: {
          frameworkMessage(event.message().executor_id(),
                           event.message().agent_id(),
                           event.message().data());
          break;
        }

        case Event::FAILURE: {
          break;
        }

        case Event::ERROR: {
          cout << event.error().message() << endl;
          process::terminate(self());
        }

        case Event::HEARTBEAT: {
          break;
        }

        case Event::UNKNOWN: {
          LOG(WARNING) << "Received an UNKNOWN event and ignored";
          break;
        }
      }
    }
  }

protected:
  virtual void initialize()
  {
    // We initialize the library here to ensure that callbacks are only invoked
    // after the process has spawned.
    mesos.reset(new scheduler::Mesos(
      master,
      mesos::ContentType::PROTOBUF,
      process::defer(self(), &Self::connected),
      process::defer(self(), &Self::disconnected),
      process::defer(self(), &Self::received, lambda::_1),
      None()));
  }

  virtual void finalize()
  {
    shutdown();

    Call call;
    CHECK(framework.has_id());
    call.mutable_framework_id()->CopyFrom(framework.id());
    call.set_type(Call::TEARDOWN);

    mesos->send(call);
  }

private:
  void doReliableRegistration()
  {
    if (state == SUBSCRIBED) {
      return;
    }

    Call call;
    if (framework.has_id()) {
      call.mutable_framework_id()->CopyFrom(framework.id());
    }
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(framework);

    mesos->send(call);

    // Re-registrate after 1 second if not subscribed then.
    process::delay(Seconds(1),
                   self(),
                   &Self::doReliableRegistration);
  }

  void resourceOffers(const vector<Offer>& offers)
  {
    foreach (const Offer& offer, offers) {
      static Resources TASK_RESOURCES = Resources::parse(
          "cpus:" + stringify<float>(CPUS_PER_TASK) +
          ";mem:" + stringify<size_t>(MEM_PER_TASK)).get();

      Resources remaining = offer.resources();
      // We can either unallocate the resources of the offer for V0
      // compatibility, or allocate the task resources with a proper role.
      remaining.unallocate();

      size_t maxTasks = 0;
      while (remaining.flatten().contains(TASK_RESOURCES)) {
        maxTasks++;
        remaining -= TASK_RESOURCES;
      }

      // Launch floor(maxTasks/2) crawlers and ceil(maxTasks/2) renderers.
      vector<TaskInfo> tasks;

      for (size_t i = 0; i < maxTasks / 2 && !crawlQueue.empty(); i++) {
        string url = crawlQueue.front();
        crawlQueue.pop();
        string urlId = "C" + stringify<size_t>(processed[url]);

        TaskInfo task;
        task.set_name("Crawler " + urlId);
        task.mutable_task_id()->set_value(urlId);
        task.mutable_agent_id()->MergeFrom(offer.agent_id());
        task.mutable_executor()->MergeFrom(crawler);
        task.mutable_resources()->MergeFrom(TASK_RESOURCES);
        task.set_data(url);
        tasks.push_back(task);

        tasksLaunched++;
        cout << "Crawler " << urlId << " " << url << endl;
      }

      for (size_t i = maxTasks / 2; i < maxTasks && !renderQueue.empty(); i++) {
        string url = renderQueue.front();
        renderQueue.pop();
        string urlId = "R" + stringify<size_t>(processed[url]);

        TaskInfo task;
        task.set_name("Renderer " + urlId);
        task.mutable_task_id()->set_value(urlId);
        task.mutable_agent_id()->MergeFrom(offer.agent_id());
        task.mutable_executor()->MergeFrom(renderer);
        task.mutable_resources()->MergeFrom(TASK_RESOURCES);
        task.set_data(url);
        tasks.push_back(task);

        tasksLaunched++;
        cout << "Renderer " << urlId << " " << url << endl;
      }

      Call call;
      CHECK(framework.has_id());
      call.mutable_framework_id()->CopyFrom(framework.id());
      call.set_type(Call::ACCEPT);

      Call::Accept* accept = call.mutable_accept();
      accept->add_offer_ids()->CopyFrom(offer.id());

      Offer::Operation* operation = accept->add_operations();
      operation->set_type(Offer::Operation::LAUNCH);
      foreach (const TaskInfo& task, tasks) {
        operation->mutable_launch()->add_task_infos()->CopyFrom(task);
      }

      mesos->send(call);
    }
  }

  void statusUpdate(const TaskStatus& status)
  {
    if (status.has_uuid()) {
      Call call;
      CHECK(framework.has_id());
      call.mutable_framework_id()->CopyFrom(framework.id());
      call.set_type(Call::ACKNOWLEDGE);

      Call::Acknowledge* ack = call.mutable_acknowledge();
      ack->mutable_agent_id()->CopyFrom(status.agent_id());
      ack->mutable_task_id()->CopyFrom(status.task_id());
      ack->set_uuid(status.uuid());

      mesos->send(call);
    }

    if (status.state() == TASK_FINISHED) {
      cout << "Task " << status.task_id().value() << " finished" << endl;
      tasksFinished++;
    }

    if (tasksFinished == tasksLaunched &&
        crawlQueue.empty() &&
        renderQueue.empty()) {
      // We don't wait to receive pending framework messages, if any. Framework
      // messages are not reliable, so we can't easily recover dropped messages
      // anyway.
      int missing_messages = tasksFinished - frameworkMessagesReceived;
      if (missing_messages > 0) {
        cout << "Noticed that " << missing_messages
             << " framework messages were not received" << endl;
      }

      process::terminate(self());
    }
  }

  void frameworkMessage(const ExecutorID& executorId,
                        const AgentID& agentId,
                        const string& data)
  {
    vector<string> strVector = stringToVector(data);
    string taskId = strVector[0];
    string url = strVector[1];

    if (executorId.value() == crawler.executor_id().value()) {
      cout << "Crawler msg received: " << taskId << endl;

      for (size_t i = 2; i < strVector.size(); i++) {
        string& newUrl = strVector[i];
        crawlResults[url].push_back(newUrl);
        if (processed.count(newUrl) == 0) {
          processed[newUrl] = nextUrlId++;
          size_t scheme = newUrl.find_first_of("://");
          size_t lsp = newUrl.find_last_of(
              '/', scheme == string::npos ? 0 : scheme + 3); // skip the http:// part
          if (newUrl.substr(0, lsp) == baseUrl) {
            crawlQueue.push(newUrl);
          }
          renderQueue.push(newUrl);
        }
      }
    } else {
      if (access(strVector[2].c_str(), R_OK) == 0) {
        renderResults[url] = strVector[2];
      }
    }
    frameworkMessagesReceived++;
  }

  FrameworkInfo framework;
  const ExecutorInfo crawler;
  const ExecutorInfo renderer;
  const string master;
  process::Owned<scheduler::Mesos> mesos;

  enum State
  {
    INITIALIZING = 0,
    CONNECTED = 1,
    SUBSCRIBED = 2,
    DISCONNECTED = 3
  } state;

  string seedUrl;
  string baseUrl;
  size_t tasksLaunched;
  size_t tasksFinished;
  size_t frameworkMessagesReceived;
};


static void shutdown()
{
  printf("RendlerV1 is shutting down\n");
  printf("Writing results to result.dot\n");

  FILE *f = fopen("result.dot", "w");
  fprintf(f, "digraph G {\n");
  fprintf(f, "  node [shape=box];\n");

  // Add vertices.
  map<string, string>::iterator rit;
  for (rit = renderResults.begin(); rit != renderResults.end(); rit++) {
    // Prepend character as dot vertices cannot starting with a digit.
    string url_hash = "R" + stringify<size_t>(processed[rit->first]);
    string& filename = rit->second;
    fprintf(f,
            "  %s[label=\"\" image=\"%s\"];\n",
            url_hash.c_str(),
            filename.c_str());
  }

  // Add edges.
  map<string, vector<string> >::iterator cit;
  for (cit = crawlResults.begin(); cit != crawlResults.end(); cit++) {
    if (renderResults.find(cit->first) == renderResults.end()) {
      continue;
    }
    string from_hash = "R" + stringify<size_t>(processed[cit->first]);
    vector<string>& adjList = cit->second;

    for (size_t i = 0; i < adjList.size(); i++) {
      string to_hash = "R" + stringify<size_t>(processed[adjList[i]]);
      if (renderResults.find(adjList[i]) != renderResults.end()) {
        // DOT format is:
        // A -> B;
        fprintf(f, "  %s -> %s;\n", from_hash.c_str(), to_hash.c_str());
      }
    }
  }

  fprintf(f, "}\n");
  fclose(f);
}


static void SIGINTHandler(int signum)
{
  if (rendler.get()) {
    process::terminate(rendler->self());
  }
  rendler.reset();
  exit(0);
}


#define shift argc--,argv++
int main(int argc, char** argv)
{
  string seedUrl, master;
  shift;
  while (true) {
    string s = argc>0 ? argv[0] : "--help";
    if (argc > 1 && s == "--seedUrl") {
      seedUrl = argv[1];
      shift; shift;
    } else if (argc > 1 && s == "--master") {
      master = argv[1];
      shift; shift;
    } else {
      break;
    }
  }

  if (master.length() == 0 || seedUrl.length() == 0) {
    printf("Usage: rendler_v1 --seedUrl <URL> --master <ip>:<port>\n");
    exit(1);
  }

  // Find this executable's directory to locate executor.
  string path = realpath(dirname(argv[0]), NULL);
  string crawlerUri = path + "/crawl_v1_executor";
  string rendererUri = path + "/render_v1_executor";
  cout << crawlerUri << endl;
  cout << rendererUri << endl;

  // Set up the signal handler for SIGINT for clean shutdown.
  struct sigaction action;
  action.sa_handler = SIGINTHandler;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;
  sigaction(SIGINT, &action, NULL);

  process::initialize();

  const Result<string> user = os::user();
  CHECK_SOME(user);

  FrameworkInfo framework;
  framework.set_user(user.get());
  framework.set_name("RendlerV1 Framework (C++)");
  //framework.set_role(role);
  framework.set_principal("rendler-cpp");

  ExecutorInfo crawler;
  crawler.mutable_executor_id()->set_value("Crawler");
  crawler.mutable_command()->set_value(crawlerUri);
  crawler.set_name("Crawl Executor (C++)");
  crawler.set_source("cpp");

  ExecutorInfo renderer;
  renderer.mutable_executor_id()->set_value("Renderer");
  renderer.mutable_command()->set_value(rendererUri);
  renderer.set_name("Render Executor (C++)");
  renderer.set_source("cpp");

  rendler.reset(new RendlerV1(framework, crawler, renderer, master, seedUrl));

  process::spawn(rendler.get());
  process::wait(rendler.get());

  return 0;
}
