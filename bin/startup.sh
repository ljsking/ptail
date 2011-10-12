process_nu=10
for ((  i = 0 ;  i < $process_nu;  i++  ))
do
    ./ptail /scribedata/bmt_$i/tmove09-1.nm/ | python ../py/validator.py --logfile=../validator$i.log &
done
