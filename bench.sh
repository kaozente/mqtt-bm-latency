set -x

SERVER=localhost

mqtt pub --topic "SETTING/SET/cache_reservations" --message "true" -h $SERVER
mqtt pub --topic "SETTING/SET/cache_subscriptions" --message "true" -h $SERVER

mqtt pub --topic "SETTING/SET/filter_on_publish" --message "true"  -h $SERVER
mqtt pub --topic "SETTING/SET/filter_on_subscribe" --message "false"  -h $SERVER
mqtt pub --topic "SETTING/SET/filter_hybrid" --message "false"  -h $SERVER
mqtt pub --topic "SETTING/SET/use_tree_store" --message "true"  -h $SERVER
sleep 1
mqtt pub --topic "SETTING/RESET" --message ""  -h $SERVER
sleep 3

echo "presub..."
mqtt pub --topic "RESERVE/bench/HASH{benchmarking}" --message ""  -h $SERVER
for i in {0..9}                                                                                                                                            
do
	mqtt pub --topic "SETTING/PRESUB/mqtt-benchmark-$i/bench/bm-$i{benchmarking/bm}" --message "" -h $SERVER
done

echo "bench..."
./mqtt-bm-latency -count 100000 -clients 1 -topic "bench/bm" -broker tcp://$SERVER:1883
./mqtt-bm-latency -count 100000 -clients 10 -topic "bench/bm" -broker tcp://$SERVER:1883
./mqtt-bm-latency -count 20000 -clients 25 -topic "bench/bm" -broker tcp://$SERVER:1883
