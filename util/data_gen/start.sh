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