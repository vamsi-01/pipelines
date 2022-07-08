#!/bin/bash
#
# Copyright 2021-2022 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

source_root=$(pwd)

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null && pwd)"
cd "${DIR}"

for ARGUMENT in "$@"
do
   KEY=$(echo $ARGUMENT | cut -f1 -d=)

   KEY_LENGTH=${#KEY}
   VALUE="${ARGUMENT:$KEY_LENGTH+1}"

   export "$KEY"="$VALUE"
done

export KFP_ENDPOINT="https://$(curl https://raw.githubusercontent.com/kubeflow/testing/master/test-infra/kfp/endpoint)"

if [ "$SOURCE_CHANGE" = sdk ]; then
  echo "Installing kfp from current commit..."
  pushd $source_root
  python3 -m pip install -e ./sdk/python
  popd
else
  echo "Installing kfp pre-release version"
  python3 -m pip install --pre kfp
fi

if [ "$SOURCE_CHANGE" = backend ]; then
  echo "Not sure what to do about this..."
else
  echo "Using existing endpoint ${KFP_ENDPOINT}"
fi

python3 -m pip install pytest pytest-asyncio-cooperative pytest-sugar

python3 sample_test.py
