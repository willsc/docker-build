DeclareParameters() {
    read -p "Search all CNEs for flows to/from (enter) IP address: " ipaddr

    # Let the user specify 5, 10, or 15 minutes prior to the current time
    read -p "Specify time offset (5, 10, or 15 minutes prior to now): " offset

    # Validate the input to ensure it's 5, 10, or 15
    if [[ "$offset" != "5" && "$offset" != "10" && "$offset" != "15" ]]; then
        echo "Invalid input. Please enter 5, 10, or 15."
        exit 1
    fi

    # Calculate the start and end times
    endtime=$(date +"%Y-%m-%d %H:%M:%S")
    starttime=$(date -d "-$offset minutes" +"%Y-%m-%d %H:%M:%S")

    echo "Start time: $starttime"
    echo "End time: $endtime"

    user="admin"
    stty -echo
    read -p "Enter password (for the built-in account - enter it wrong and you will get a traceback error): " password
    stty echo

    # FlowCat uses the Corvil Python script with switches. It filters the results for TCP-only flows.
    FlowCatIT30() {
        echo "Flow on CNE BGXCNE0001 for IP $ipaddr"
        FlowCatIT3=$(python3 /path/to/CorvilApiStreamingClient.py \
            --starts "$starttime" \
            --ends "$endtime" \
            --user "$user" --password "$password" \
            -q ip.addr=$ipaddr -s -a | grep TCP | grep Port)
        echo "$FlowCatIT3"
    }

    FlowCatIT30
}
DeclareParameters