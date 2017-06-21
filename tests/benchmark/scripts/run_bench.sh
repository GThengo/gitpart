#!/bin/bash

## benchmark directory
cd ..
make clean
make

## application directory
cd ../..

mkdir -p bench_data
SAMPLES=5
for s in `seq $SAMPLES`
do
	echo -ne "UPDATE_RATE\tTIME\n" > bench_data.txt
for u in 0 10 20 30 40 50 60 70 80 90 100
do
	./tests/benchmark/bench.out -u $u >/dev/null
	echo -ne "$u\t" >> bench_data.txt
	cat benchmark_info.txt | sed -n -e 's/^.*\(time taken: \)\(.*\) s/\2/p' >> bench_data.txt
	mv benchmark_info.txt bench_data/bench_u"$u"_s"$s"
done
	mv bench_data.txt bench_data/s"$s"
done

./tests/benchmark/scripts/SCRIPT_compute_AVG_ERR.R `ls bench_data/s*`
mv avg.txt bench_data/data_proc.txt

gnuplot /tests/benchmark/scripts/plot_lines.plt



