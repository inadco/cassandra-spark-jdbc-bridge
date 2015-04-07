app_name="csjb"
csjb_pid=/tmp/inadco-csjb-server.pid


function is_running {

  if [ ! -z "$csjb_pid" ]; then
    if [ -f "$csjb_pid" ]; then
      if [ -s "$csjb_pid" ]; then
        kill -0 `cat "$csjb_pid"` >/dev/null 2>&1
        if [ $? -gt 0 ]; then
          echo "PID file found but no matching process was found. Nothing to stop."          
        fi
      else
        echo "PID file is empty and has been ignored."
      fi
    else
      echo "\$csjb_pid was set but the specified file does not exist. Is $app_name running? Stop aborted."
      exit 1
    fi
  fi


}


#Chk to see if app is running
is_running

kill $(cat /tmp/inadco-csjb-server.pid)