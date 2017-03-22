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

#include <assert.h>
#include <functional>
#include <iostream>

#include <glog/logging.h>

#include <process/defer.hpp>
#include <process/delay.hpp>

#include <stout/foreach.hpp>
#include <stout/lambda.hpp>

#include "rendler_v1_executor.hpp"

using std::cout;
using std::endl;
using std::mem_fn;


static void* start(void* arg)
{
  lambda::function<void(void)>* thunk = (lambda::function<void(void)>*) arg;
  (*thunk)();
  delete thunk;
  return NULL;
}


RendlerV1Executor::RendlerV1Executor(const string& _name,
                                     const FrameworkID& _frameworkId,
                                     const ExecutorID& _executorId)
    : frameworkId(_frameworkId),
      executorId(_executorId),
      state(INITIALIZING) {}


RendlerV1Executor::~RendlerV1Executor() {}


void RendlerV1Executor::connected()
{
  state = CONNECTED;
  doReliableRegistration();
}


void RendlerV1Executor::disconnected()
{
  state = DISCONNECTED;
}


void RendlerV1Executor::received(queue<Event> events)
{
  while (!events.empty()) {
    Event event = events.front();
    events.pop();

    switch (event.type()) {
      case Event::SUBSCRIBED: {
        cout << name << " subscribed on "
             << event.subscribed().agent_info().hostname() << endl;
        state = SUBSCRIBED;
        break;
      }

      case Event::LAUNCH: {
        launchTask(event.launch().task());
        break;
      }

      case Event::LAUNCH_GROUP: {
        const TaskGroupInfo& taskGroup = event.launch_group().task_group();
        foreach (const TaskInfo& task, taskGroup.tasks()) {
          launchTask(task);
        }
        break;
      }

      case Event::KILL: {
        break;
      }

      case Event::ACKNOWLEDGED: {
        // Remove the corresponding update.
        updates.erase(UUID::fromBytes(event.acknowledged().uuid()).get());
        // Remove the corresponding task.
        tasks.erase(event.acknowledged().task_id());
        break;
      }

      case Event::MESSAGE: {
        break;
      }

      case Event::SHUTDOWN: {
        break;
      }

      case Event::ERROR: {
        break;
      }

      case Event::UNKNOWN: {
        LOG(WARNING) << "Received an UNKNOWN event and ignored";
        break;
      }
    }
  }
}


void RendlerV1Executor::sendStatusUpdate(const TaskInfo& task,
                                         const TaskState& state)
{
  UUID uuid = UUID::random();

  TaskStatus status;
  status.mutable_task_id()->CopyFrom(task.task_id());
  status.mutable_executor_id()->CopyFrom(executorId);
  status.set_state(state);
  status.set_source(TaskStatus::SOURCE_EXECUTOR);
  status.set_uuid(uuid.toBytes());

  Call call;
  call.mutable_framework_id()->CopyFrom(frameworkId);
  call.mutable_executor_id()->CopyFrom(executorId);
  call.set_type(Call::UPDATE);

  Call::Update* update = call.mutable_update();
  update->mutable_status()->CopyFrom(status);

  // Capture the status update so we can repeat if there is no ack.
  updates[uuid] = call.update();

  mesos->send(call);
}


void RendlerV1Executor::sendFrameworkMessage(const string& data)
{
  Call call;
  call.mutable_framework_id()->CopyFrom(frameworkId);
  call.mutable_executor_id()->CopyFrom(executorId);
  call.set_type(Call::MESSAGE);

  Call::Message* message = call.mutable_message();
  message->set_data(data);

  mesos->send(call);
}


void RendlerV1Executor::initialize()
{
  // We initialize the library here to ensure that callbacks are only invoked
  // after the process has spawned.
  mesos.reset(new Mesos(
      mesos::ContentType::PROTOBUF,
      process::defer(self(), &Self::connected),
      process::defer(self(), &Self::disconnected),
      process::defer(self(), &Self::received, lambda::_1)));
}


void RendlerV1Executor::doReliableRegistration()
{
  if (state == SUBSCRIBED || state == DISCONNECTED) {
    return;
  }

  Call call;
  call.mutable_framework_id()->CopyFrom(frameworkId);
  call.mutable_executor_id()->CopyFrom(executorId);

  call.set_type(Call::SUBSCRIBE);

  Call::Subscribe* subscribe = call.mutable_subscribe();

  // Send all unacknowledged updates.
  foreachvalue (const Call::Update& update, updates) {
    subscribe->add_unacknowledged_updates()->MergeFrom(update);
  }

  // Send all unacknowledged tasks.
  foreachvalue (const TaskInfo& task, tasks) {
    subscribe->add_unacknowledged_tasks()->MergeFrom(task);
  }

  mesos->send(call);

  // Re-registrate after 1 second if not subscribed then.
  process::delay(Seconds(1), self(), &Self::doReliableRegistration);
}


void RendlerV1Executor::launchTask(const TaskInfo& task)
{
  cout << "Starting task " << task.task_id().value() << endl;

  tasks[task.task_id()] = task;

  lambda::function<void(void)>* thunk =
      new lambda::function<void(void)>(
          lambda::bind(mem_fn(&RendlerV1Executor::runTask), this, task));

  pthread_t pthread;
  if (pthread_create(&pthread, NULL, &start, thunk) != 0) {
    sendStatusUpdate(task, TASK_FAILED);
  } else {
    pthread_detach(pthread);
    sendStatusUpdate(task, TASK_RUNNING);
  }
}
