#!/bin/bash

#
# move-db2.sh
# This routine is used to move a Db2 database from OpenShift to an AWS S3 repository
#
# Date: 2020-03-25
# Credits: IBM 2020, George Baklarz [baklarz@ca.ibm.com]
#

# Set the RSYNC permissions

export OC_RSYNC_RSH="sudo -i"

# Request the name of the backup image

BACKUP="db2-$(date '+%s')"

if \
	zenity   --entry \
	         --title="Db2 OpenShift Database Move" \
	         --text="Enter a unique name for the Db2 image" \
	         --entry-text="$BACKUP" \
		2> /dev/null
	then
		BACKUP_NAME=echo $?
	else
		zenity	--info \
			--width=200 \
                        --text="Database move cancelled" \
			2> /dev/null
		exit 0
fi

(
echo "10" 
echo "# Suspending Pod Restart Service" 

oc exec db2u-db2u-0 -it -- sudo wvcli system stop

echo "20"
echo "# Stopping the database"

oc exec db2u-db2u-0 -it -- sudo -u db2inst1 /mnt/blumeta0/home/db2inst1/sqllib/adm/db2stop force

echo "40"
echo "# Extracting Pod Contents"

rm -rf ~/pod_backup
mkdir ~/pod_backup

oc rsync --strategy='tar' default/db2u-db2u-0://mnt/bludata0/db2/  ~/pod_backup

echo "60"
echo "# Restarting Services"

oc exec db2u-db2u-0 -it -- sudo wvcli system start

echo "80"
echo "# Moving Pod Contents to Cloud Repository"

sleep 5

echo "100"
echo "# Move complete" 
) |
zenity 	--progress \
  	--title="Db2 OpenShift Database Move" \
	--text="Stopping Services" \
	--percentage=10 \
	--auto-close \
	--auto-kill \
        --pulsate \
	2> /dev/null

exit 0
Step 1: Stop the running container to make it a safe move
export OC_RSYNC_RSH="sudo -i"
oc exec db2u-db2u-0 -it -- sudo wvcli system stop
oc exec db2u-db2u-0 -it -- sudo -u db2inst1 /mnt/blumeta0/home/db2inst1/sqllib/adm/db2stop
Step 2: Copy the contents of the Db2 folder to a local host folder
oc rsync --strategy='tar' default/db2u-db2u-0://mnt/bludata0/db2/  ./Downloads/db2
oc exec db2u-db2u-0 -it -- sudo wvcli system start
Step 3: Move the contents of the host folder in step 2 to the other system
....sneakernet...
Step 4: Install the Db2U on the local system and then issue Step 1 above
oc exec db2u-db2u-0 -it -- sudo wvcli system stop
oc exec db2u-db2u-0 -it -- sudo -u db2inst1 /mnt/blumeta0/home/db2inst1/sqllib/adm/db2stop
Step 5: Remove the contents of the Db2 directory in the new system
oc exec db2u-db2u-0 -it -- sudo  rm -Rf //mnt/bludata0/db2
Step 6: Copy the contents of the local Db2 folder into the pod
oc exec db2u-db2u-0 -it -- sudo  mkdir  //mnt/bludata0/db2
oc rsync  --strategy='tar' ./Downloads/db2/ default/db2u-db2u-0://mnt/bludata0/db2
Step 7: Change the ownership permissions because of rsync issues
oc exec db2u-db2u-0 -it -- sudo  chown -Rf db2inst1:db2iadm1 /mnt/bludata0/db2
Step 8: Restart the Db2 instance
oc exec db2u-db2u-0 -it -- sudo wvcli system start



