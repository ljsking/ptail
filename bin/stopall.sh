ps -ef | grep ptail | grep -v grep | awk '{print $2}' | xargs kill -9
