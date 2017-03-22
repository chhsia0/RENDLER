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

#include <iostream>
#include <vector>

#include <boost/regex.hpp>

#include <curl/curl.h>

#include <stout/os.hpp>

#include "rendler_helper.hpp"
#include "rendler_v1_executor.hpp"

using std::cout;
using std::endl;
using std::vector;

using mesos::vectorToString;


static int writer(char *data, size_t size, size_t nmemb, string *writerData)
{
  assert(writerData != NULL);
  writerData->append(data, size*nmemb);
  return size * nmemb;
}


class CrawlV1Executor : public RendlerV1Executor
{
public:
  CrawlV1Executor(const FrameworkID& _frameworkId,
                  const ExecutorID& _executorId)
    : RendlerV1Executor("CrawlV1Executor", _frameworkId, _executorId) {}

  virtual ~CrawlV1Executor() {}

protected:
  void runTask(const TaskInfo& task) override
  {
    string url = task.data();
    cout << "Running crawl task " << task.task_id().value()
         << " Fetch: " << url;

    string buffer;
    vector<string> result;
    result.push_back(task.task_id().value());
    result.push_back(url);

    CURL *conn;
    conn = curl_easy_init();
    assert(conn != NULL);
    assert(curl_easy_setopt(conn, CURLOPT_URL, url.c_str()) == CURLE_OK);
    assert(curl_easy_setopt(conn, CURLOPT_FOLLOWLOCATION, 1L) == CURLE_OK);
    assert(curl_easy_setopt(conn, CURLOPT_WRITEFUNCTION, writer) == CURLE_OK);
    assert(curl_easy_setopt(conn, CURLOPT_WRITEDATA, &buffer) == CURLE_OK);

    if (curl_easy_perform(conn) != CURLE_OK) {
      return;
    }

    char *tmp;
    assert(curl_easy_getinfo(conn, CURLINFO_EFFECTIVE_URL, &tmp) == CURLE_OK);
    string redirectUrl = url;
    if (tmp != NULL) {
      redirectUrl = tmp;
    }
    curl_easy_cleanup(conn);

    size_t scheme = redirectUrl.find_first_of("://");
    size_t sp = redirectUrl.find_first_of('/',
        scheme == string::npos ? 0 : scheme + 3); // skip the http:// part.
    size_t lsp = redirectUrl.find_last_of('/'); // skip the http:// part.
    string baseUrl = redirectUrl.substr(0, sp); // No trailing slash.
    string dirUrl = redirectUrl.substr(0, lsp); // No trailing slash.

    cout << "redirectUrl " << redirectUrl << " baseURL: " << baseUrl << endl;
    cout << "dirUrl " << dirUrl  << endl;

    const boost::regex hrefRE("<a\\s+[^\\>]*?href\\s*=\\s*([\"'])(.*?)\\1");
    const boost::regex urlRE("^([a-zA-Z]+://).*");

    boost::smatch matchHref;
    string::const_iterator f = buffer.begin();
    string::const_iterator l = buffer.end();

    while (f != buffer.end() &&
           boost::regex_search(f, l, matchHref, hrefRE)) {
      string link = matchHref[2];
      f = matchHref[0].second;

      boost::smatch matchService;
      string::const_iterator lb = link.begin();
      string::const_iterator le = link.end();

      // Remove the anchor
      if (link.find_first_of('#') != string::npos) {
        link.erase(link.find_first_of('#'));
      }
      if (link.empty()) {
        continue;
      }
      if (link[0] == '/') {
        link = baseUrl + link;
      } else if (!boost::regex_search(lb, le, matchService, urlRE)) {
        // Relative URL
        link = dirUrl + "/" + link;
      }
      result.push_back(link);
    };

    sendFrameworkMessage(vectorToString(result));
    sendStatusUpdate(task, TASK_FINISHED);
  }
};


int main(int argc, char** argv)
{
  FrameworkID frameworkId;
  ExecutorID executorId;

  Option<string> value;

  value = os::getenv("MESOS_FRAMEWORK_ID");
  if (value.isNone()) {
    EXIT(EXIT_FAILURE)
      << "Expecting 'MESOS_FRAMEWORK_ID' to be set in the environment";
  }
  frameworkId.set_value(value.get());

  value = os::getenv("MESOS_EXECUTOR_ID");
  if (value.isNone()) {
    EXIT(EXIT_FAILURE)
      << "Expecting 'MESOS_EXECUTOR_ID' to be set in the environment";
  }
  executorId.set_value(value.get());

  process::Owned<CrawlV1Executor> crawler(
      new CrawlV1Executor(frameworkId, executorId));

  process::spawn(crawler.get());
  process::wait(crawler.get());

  return 0;
}
