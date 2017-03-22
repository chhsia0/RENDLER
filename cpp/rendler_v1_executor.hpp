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

#ifndef __RENDLER_V1_EXECUTOR_HPP__
#define __RENDLER_V1_EXECUTOR_HPP__

#include <queue>
#include <string>

#include <mesos/v1/executor.hpp>

#include <process/owned.hpp>
#include <process/process.hpp>

#include <stout/hashmap.hpp>
#include <stout/uuid.hpp>

using namespace mesos::v1;

using std::queue;
using std::string;

using mesos::v1::executor::Call;
using mesos::v1::executor::Event;
using mesos::v1::executor::Mesos;


class RendlerV1Executor : public process::Process<RendlerV1Executor>
{
public:
  RendlerV1Executor(const string& _name,
                    const FrameworkID& _frameworkId,
                    const ExecutorID& _executorId);
  virtual ~RendlerV1Executor();

  void connected();
  void disconnected();
  void received(queue<Event> events);
  void sendStatusUpdate(const TaskInfo& task, const TaskState& state);
  void sendFrameworkMessage(const string& data);

protected:
  virtual void initialize();
  virtual void runTask(const TaskInfo& task) = 0;

private:
  void doReliableRegistration();
  void launchTask(const TaskInfo& task);

  const string name;
  const FrameworkID frameworkId;
  const ExecutorID executorId;
  process::Owned<Mesos> mesos;

  enum State
  {
    INITIALIZING = 0,
    CONNECTED = 1,
    SUBSCRIBED = 2,
    DISCONNECTED = 3
  } state;

  hashmap<UUID, Call::Update> updates; // Unacknowledged updates.
  hashmap<TaskID, TaskInfo> tasks; // Unacknowledged tasks.
};

#endif // __RENDLER_V1_EXECUTOR_HPP__
