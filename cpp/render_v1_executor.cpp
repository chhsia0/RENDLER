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

static string renderJSPath;
static string workDirPath;


class RenderV1Executor : public RendlerV1Executor
{
public:
  RenderV1Executor(const FrameworkID& _frameworkId,
                   const ExecutorID& _executorId)
    : RendlerV1Executor("RenderV1Executor", _frameworkId, _executorId) {}

  virtual ~RenderV1Executor() {}

protected:
  void runTask(const TaskInfo& task) override
  {
    string url = task.data();
    cout << "Running render task (" << task.task_id().value() << "): " << url;
    string filename = workDirPath + task.task_id().value() + ".png";

    vector<string> result;
    result.push_back(task.task_id().value());
    result.push_back(url);
    result.push_back(filename);

    string cmd = "QT_QPA_PLATFORM=offscreen phantomjs " + renderJSPath + " " + url + " " + filename;
    assert(system(cmd.c_str()) != -1);

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

  std::string path = os::realpath(::dirname(argv[0])).get();
  renderJSPath = path + "/render.js";
  workDirPath = path + "/rendler-work-dir/";

  process::Owned<RenderV1Executor> renderer(
      new RenderV1Executor(frameworkId, executorId));

  process::spawn(renderer.get());
  process::wait(renderer.get());

  return 0;
}
