# /*
#  * Copyright 2021 Cloudera, Inc.
#  *
#  * Licensed under the Apache License, Version 2.0 (the "License");
#  * you may not use this file except in compliance with the License.
#  * You may obtain a copy of the License at
#  *
#  *   http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */

step=$1

if [[ "$step" == 1 ]]; then
echo "Step 1. Produce data for 2022-01-01 and 2022-01-02"
python generate_data.py --days 2 --start-date 2022-01-01
python write_data.py --refresh
elif [[ "$step" == 2 ]]; then
echo "Step 2. Produce data for 2022-01-03 and 2022-01-04"
python generate_data.py --days 2 --start-date 2022-01-03
python write_data.py
fi