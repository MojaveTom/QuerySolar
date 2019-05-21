#!/bin/bash
#
#
#    StartQuerySolar.sh -- Shell script to kill and restart QuerySolar.py
#      program every day at 2315.
#    Copyright (C) 2015  Thomas A. DeMay
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 3 of the License, or
#    any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License along
#    with this program; if not, write to the Free Software Foundation, Inc.,
#    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
# *
# */
#
#  This script creates a special file in the user's home directory that the
# program looks for.  When found, the program quits gracefully.
#
#    Script is run in the source directory.
#
programName=QuerySolar
meScript=${0##*/}
echo "This file is: $meScript"
declare -i waitCount=20     #  Will cause us to give up clean quit after 200 seconds.
# Get pid of process.
# Look for at the end of the process status entry
# wpid is NOT empty if the application is running, and the status is true.
if wpid=$(pgrep -f "${programName}.py$")
then
    touch $HOME/.Close${programName}       # Create .Close${programName} file if not exist.
    while wpid=$(pgrep -f "${programName}.py$")
    do      ##  Wait for TimeSyncServer program to see .Close${programName} file and quit.
        sleep 10
        ## But don't wait forever.
        if [ $((--waitCount)) -lt 0 ]; then break; fi
    done
fi

# if wpid is not empty, clean termination above did not work.
if [ -n "$wpid" ]; then
    kill $wpid                          # kill the process
fi

rm -f $HOME/.Close${programName}    # remove the quit file
                                    # so it won't kill the process
                                    # immediately after starting.

# sleep for 10 seconds
sleep 10

# ## if there is an "at" job scheduled for this program, remove it before starting another.
# for j in $(at -l | cut -f1)  # get a list of pids for my "at" jobs.
# do
#   # The last lines of the script restart it; extract the file name from the script.
#   jobFile=$(at -c $j | tail -n4 | grep 'at -f.*.sh' | sed -E s/'at -f(.*sh ).*/\1/')
#   if [ -n "$jobFile" ]      # ignore empty jobFiles
#   then
#     echo "Checking to see if $jobFile is us."
#     if [ "$jobFile" = "$meScript" ]      # if the file name from the "at" script matches us
#     then                            # remove it from the list
# #      echo "$jobFile is pid $j"
#       at -r $j
#     fi
#   fi
# done

# restart ${programName}.py program
####   MAKE SURE THERE IS A LINK TO THE ${programName}.py EXECUTABLE
#### WHERE THIS SCRIPT EXPECTS IT TO BE.
$PWD/${programName}.py &

# Reschedule this script to run at 2305 tomorrow.
#  Have to put in actual script name here so job killer above can find it.
at -fStartQuerySolar.sh 2305 >/dev/null 2>&1
