set term postscript color eps enhanced 22
set output sprintf("|ps2pdf -dEPSCrop - plot.pdf")

set grid ytics
set grid xtics

set ytics font ",10pt" offset 0.5,0.0
set xtics font ",10pt" offset 0.0,0.5

set lmargin at screen 0.06
set rmargin at screen 0.983
set bmargin at screen 0.08
set tmargin at screen 0.90

# set logscale x 2
set mxtics

### TODO:

set key at graph 0.18,0.98 font ",12pt" spacing 1
set ylabel font ",12pt" offset 4.3,0.0 "Response Time (s)"
set xlabel font ",12pt" offset 0.0,1.2 "Update rate (%)"
set title sprintf("SOME TITLE")

DATA=sprintf("./bench_data/data_proc.txt")

plot DATA u 1:2:($2-$4):($2+$4) title 'SOME\_TITLE' w yerrorbars lc 1 \
     DATA u 1:2                 notitle w lines lc 1 
  
