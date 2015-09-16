app_name=csjb
csjb_pid=/tmp/inadco-csjb-server.pid

#Chk to see if app is running
function is_running {
  if [ ! -z "$csjb_pid" ]; then
    if [ -f "$csjb_pid" ]; then
      if [ -s "$csjb_pid" ]; then
        echo "Existing PID file found during start."
        if [ -r "$csjb_pid" ]; then
          PID=`cat "$csjb_pid"`
          ps -p $PID >/dev/null 2>&1
          if [ $? -eq 0 ] ; then
            echo "$app_name appears to still be running with PID $PID. Start aborted."
            exit 1
          else
            echo "Removing/clearing stale PID file."
            rm -f "$csjb_pid" >/dev/null 2>&1
            if [ $? != 0 ]; then
              if [ -w "$csjb_pid" ]; then
                cat /dev/null > "$csjb_pid"
              else
                echo "Unable to remove or clear stale PID file. Start aborted."
                exit 1
              fi
            fi
          fi
        else
          echo "Unable to read PID file. Start aborted."
          exit 1
        fi
      else
        rm -f "$csjb_pid" >/dev/null 2>&1
        if [ $? != 0 ]; then
          if [ ! -w "$csjb_pid" ]; then
            echo "Unable to remove or write to empty PID file. Start aborted."
            exit 1
          fi
        fi
      fi
    fi
  fi

 }
  

function get_master {
    master=`cat $INADCO_CSJB_HOME/config/csjb-default.properties | grep inadco.spark.master  | cut -d "=" -f2`
}

#Chk to see if app is running
is_running


echo "Starting $app_name"

if [[ -z "$SPARK_HOME" ]]; then
        echo "SPARK_HOME is not set.";
        exit 1;
fi

if [[ -z "$INADCO_CSJB_HOME" ]]; then
        echo "INADCO_CSJB_HOME is not set.";
        exit 1;
fi

mkdir -p $INADCO_CSJB_HOME/var/log
if [[ -d $INADCO_CSJB_HOME/var/log ]]; then
		echo "Creating log dir $INADCO_CSJB_HOME/var/log";
        
fi

get_master

$SPARK_HOME/bin/spark-submit --class com.inadco.cassandra.spark.jdbc.InadcoCSJServer \
--master $master $INADCO_CSJB_HOME/inadco-csjb-assembly-1.0.jar >>$INADCO_CSJB_HOME/var/log/log.out \
2>>$INADCO_CSJB_HOME/var/log/log.err & echo $! > $csjb_pid