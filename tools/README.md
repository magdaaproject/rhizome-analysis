# Replication Graph #

The small sample program, `replication.c`, in this directory can be used to take the output of the `chart-bundles-over-time` task to create a chart using [R][r-project]. 

To create the chart follow these steps:

1. Copy the output file to `data/test.csv`, and remove all quotes, and replace all commas with semicolons
2. Compile and run the `replication.c` program
3. Run the following [R][r-project] code to produce the PDF
<pre>
data(replicationcurve)
pdf(file="replication.pdf")
plot(replicationcurve,ylim=c(0,20),type="l")
dev.off()
</pre>

[r-project]: http://www.r-project.org/