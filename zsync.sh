#!/bin/sh

VERSION="1.0"

trap 'echo -e "\n転送が中断されました。"; stty sane; exit 1' INT

USE_MBUFFER=0

ZFS_SEND_OPTION="-v"

REMOTE_USER_HOST=""
LOCAL_DATASET=""
REMOTE_DATASET=""

RECORD_SIZE=""
BUFFER_SIZE=""
TRANSFER_RATE="-R 20m"

PREFIX="zsync-$(hostname)"

#!/bin/bash

show_usage() {
    echo $0 version $VERSION 
    echo "Usage: $0 [-m BUFFER_SIZE] [-R TRANSFER_RATE] [-P PREFIX] REMOTE_USER_HOST LOCAL_DATASET [REMOTE_DATASET]"
    echo "   or: $0 [-m BUFFER_SIZE] [-R TRANSFER_RATE] [-P PREFIX] LOCAL_DATASET REMOTE_USER_HOST [REMOTE_DATASET]"
}

while [ "$#" -gt 0 ]; do
	case "$1" in
	    -P) shift; PREFIX="$1"		; shift ;;
	    -B) shift; USE_MBUFFER=1 		; shift ;;
	    -s) shift; RECORD_SIZE="-s $1"	; shift ;;
	    -m) shift; BUFFER_SIZE="-m $1"	; shift ;;
	    -R) shift; TRANSFER_RATE="-R $1"	; shift ;;
	    -V) 
	    	echo $0 version $VERSION 
	    	exit 0 ;;
	    -h) 
	    	show_usage 
	    	exit 0 ;;
	    *)
	        if [ -z "$LOCAL_DATASET" ]; then
	            if echo "$1" | grep -qE "[^@]+@[^@]+"; then
	                REMOTE_USER_HOST="$1"
	            else
	                LOCAL_DATASET="$1"
	            fi
	        elif [ -z "$REMOTE_USER_HOST" ]; then
	            REMOTE_USER_HOST="$1"
	        elif [ -z "$REMOTE_DATASET" ]; then
	            REMOTE_DATASET="$1"
	        else
	            echo "Unknown argument: $1"
	            exit 1
	        fi
	        shift ;;
	esac
done

if [ -z "$REMOTE_DATASET" ]; then
    REMOTE_DATASET="$LOCAL_DATASET"
fi

if [ "$USE_MBUFFER" -eq 1 ]; then
    echo "mbuffer using"
   MBUFFER_CMD="mbuffer -q $BUFFER_SIZE $TRANSFER_RATE $RECORD_SIZE "
else
    echo "mbuffer off"
    MBUFFER_CMD="cat"
fi

# Check for required parameters
if [ -z "$LOCAL_DATASET" ] || [ -z "$REMOTE_USER_HOST" ]; then
    show_usage
    exit 1
fi

# Create a new local snapshot with a prefix
LATEST_LOCAL_SNAPSHOT="$PREFIX-$(date +%Y%m%d-%H%M%S)"
sudo zfs snapshot "$LOCAL_DATASET@$LATEST_LOCAL_SNAPSHOT"

# Get the latest remote snapshot
PREVIOUS_REMOTE_SNAPSHOT=$(ssh "$REMOTE_USER_HOST" "sudo zfs list -t snapshot -H -o name" | grep "$REMOTE_DATASET@$PREFIX" | sort -n | tail -1)

# If there is no previous remote snapshot, do a full send
if [ -z "$PREVIOUS_REMOTE_SNAPSHOT" ]; then
	# Get the receive_resume_token
	RECEIVE_RESUME_TOKEN=$(ssh "$REMOTE_USER_HOST" "zfs get -H -o value receive_resume_token $REMOTE_DATASET" 2>/dev/null || true)
	if [ -n "${RECEIVE_RESUME_TOKEN}" ] && [ "$RECEIVE_RESUME_TOKEN" != "-" ]; then
	
	    # If there is no previous remote snapshot, but the receive_resume_token is valid, resume transfer using receive_resume_token
		
            echo "Resuming transfer using receive_resume_token: $RECEIVE_RESUME_TOKEN"
		sudo zfs send $ZFS_SEND_OPTION -t "$RECEIVE_RESUME_TOKEN" | $MBUFFER_CMD | \
		ssh "$REMOTE_USER_HOST" "$MBUFFER_CMD | sudo zfs receive -s -F $REMOTE_DATASET"
	else
	    echo "Performing initial full send of $LOCAL_DATASET@$LATEST_LOCAL_SNAPSHOT of $REMOTE_USER_HOST"
		sudo zfs send $ZFS_SEND_OPTION "$LOCAL_DATASET@$LATEST_LOCAL_SNAPSHOT" | $MBUFFER_CMD | \
		ssh "$REMOTE_USER_HOST" "$MBUFFER_CMD | sudo zfs receive -s -F $REMOTE_DATASET"
	fi
else
    	REMOTE_ESCAPED=$(echo "$REMOTE_DATASET" | sed -e 's/\//\\\//g')
    	LOCAL_ESCAPED=$(echo "$LOCAL_DATASET" | sed -e 's/\//\\\//g')
    	PREVIOUS_LOCAL_SNAPSHOT=$(echo "$PREVIOUS_REMOTE_SNAPSHOT" | sed "s/$REMOTE_ESCAPED/$LOCAL_ESCAPED/g")
    	
	# If there is a previous remote snapshot, send only the incremental changes
	
	echo "Sending incremental changes from $PREVIOUS_REMOTE_SNAPSHOT to $REMOTE_DATASET@$LATEST_LOCAL_SNAPSHOT of $REMOTE_USER_HOST"
 	    	sudo zfs send "$ZFS_SEND_OPTION" -i "$PREVIOUS_LOCAL_SNAPSHOT" "$LOCAL_DATASET@$LATEST_LOCAL_SNAPSHOT" | $MBUFFER_CMD | \
		ssh "$REMOTE_USER_HOST" "$MBUFFER_CMD | sudo zfs receive -F $REMOTE_DATASET"
     
	# Check the result of the send operation and delete snapshots
	
	if [ "$?" -eq 0 ]; then
	        sudo zfs destroy "$PREVIOUS_LOCAL_SNAPSHOT"
	        ssh "$REMOTE_USER_HOST" "sudo zfs destroy $PREVIOUS_REMOTE_SNAPSHOT"
	else
	        echo "Error: zfs send failed"
	fi
fi
