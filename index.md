---
layout: default
---

<!-- <div style="text-align:center;">
  <img class="img-spark-flink" src="/ApproxIoT/images//spark-flink1.png" alt="spark-flink" style="height: 80px; weight: 800px;"/>
</div> -->

<div id="container">
  <div id="row">
  <div class="child" markdown="0">
          <a href="https://xxx">
              <img class="t0" width="40%" src="/ApproxIoT/images/IEEE.png" alt="Paper">
              <div style="text-align:left; margin: 0 0 0 0; font-size: 0.8em;">Paper</div>
          </a>
  </div>
    <div class="child" markdown="0">   
     <a href="https://arxiv.org/abs/1805.05674">
            <img class="t0" width="40%" src="/ApproxIoT/images/report-icon.png" alt="Technical report">
            <div style="text-align:left; margin: 0 0 0 0; font-size: 0.8em;">Technical report</div>
        </a></div>

  <div class="child" markdown="0">
  <a href="https://approxiot.github.io/ApproxIoT/docs/bib.md">
    <img class="t0" width="40%" src="/ApproxIoT/images/bibtex-icon.png" alt="Bibtex">
    <div style="text-align:left; margin: 0 0 0 0; font-size: 0.8em;">BibTex</div>
  </a></div>

    <div class="child" markdown="0">
    <a href="https://github.com/ApproxIoT/ApproxIoT/tree/master/code">
        <img class="t0" width="40%" src="/ApproxIoT/images/github-icon.png" alt="Source code">
        <div style="text-align:left; margin: 0 0 0 0; font-size: 0.8em;">Source code</div>
    </a>
    </div>
  </div>
</div>



<!-- <div class="row">
<div class="large-2 large-push columns" markdown="0" style="display: inline-block;">
        <a href="https://xxx">
            <img class="t0" width="15%" src="/ApproxIoT/images/IEEE.png" alt="Paper">
            <div style="text-align:left; margin: 0 0 0 0; font-size: 0.8em;">Paper</div>
        </a>
</div>

 <div class="large-2 large-push-2 columns" markdown="0" style="display: inline-block;">
        <a href="https://xx">
            <img class="t0" width="8%" src="/ApproxIoT/images/report-icon.png" alt="Technical report">
            <div style="text-align:left; margin: 0 0 0 0; font-size: 0.8em;">Technical report</div>
        </a>
</div>

 <div class="large-2 large-push-2 columns" markdown="0" style="display: inline-block;">
        <a href="/ApproxIoT/docs/bib.md">
            <img class="t0" width="23%" src="/ApproxIoT/images/bibtex-icon.png" alt="Bibtex">
            <div style="text-align:left; margin: 0 0 0 0; font-size: 0.8em;">BibTex</div>
        </a>
</div>

 <div class="large-2 large-push-2 columns" markdown="0" style="display: inline-block;">
        <a href="https://xx">
            <img class="t0" width="12%" src="/ApproxIoT/images/github-icon.png" alt="Source code">
            <div style="text-align:left; margin: 0 0 0 0; font-size: 0.8em;">Source code</div>
        </a>
  </div>
</div> -->
-------
<!-- <div class="large-2 medium-push-2 columns" style="text-align:center;position:relative;left:29%;right:auto;">
        <a href="/slides/Middleware17.pptx">
            <img class="t0" width="45%" src="/images/pptx-icon.png" alt="Middleware"> <div style="text-align:center; margin: 0 0 0 0; font-size: 0.8em;">Middleware'17</div>
        </a>
</div>

<div class="large-2 medium-push-2 columns" style="text-align:center;position:relative;left:29%;right:auto;">
        <a href="/slides/FlinkForward17.pptx">
            <img class="t0" width="45%" src="/images/pptx-icon.png" alt="Flink Forward">
            <div style="text-align:center; margin: 0 0 0 0; font-size: 0.8em;">Flink Forward'17</div>
        </a>
</div>

<div class="large-2 medium-push-2 columns" style="text-align:center;position:relative;left:29%;right:auto;">
        <a href="/slides/SparkSummit17.pptx">
            <img class="t0" width="45%" src="/images/pptx-icon.png" alt="Spark Summit">
            <div style="text-align:center; margin: 0 0 0 0; font-size: 0.8em;">Spark Summit'17</div>
        </a>
</div> -->

<!-- <div style="text-align:center; font-size: 0.9em; border-bottom: 3px double #8c8b8b;">
        <div style="text-align:center; margin: 0 0 0 0; font-size: 0.5em;">
        <img width="6%" src="/images/pptx-icon.png" alt="Slides">
        </div>
        <a href="/slides/Middleware17.pptx">Middleware'17</a>|
        <a href="/slides/FlinkForward17.pptx">Flink Forward'17</a>|
        <a href="/slides/SparkSummit17.pptx">Spark Summit'17</a>
</div>  -->


# Introduction
Approximate computing aims for efficient execution of workflows where an approximate output is sufficient instead of the exact output. Thus, approximate computing based on the chosen sample size — can make a systematic tradeoff between the output accuracy and computation efficiency.
ApproxIoT is a stream analytics system to strike a balance between the two desirable but contradictory design requirements, i.e., achieving low latency for real-time analytics, and efficient utilization of computing resources.

<div>
  <img style="text-align:center;" class="img-overivew" src="/ApproxIoT/images/overview.png" alt="overview" style="height: 300px; weight: 1000px;"/>
</div>


In this work, we implemented ApproxIoT using Apache Kafka and its library Kafka Streams to achieve a truly distributed data analytics system. An online stratified reservoir sampling algorithm was implemented on both Edge computing nodes and Datacenter cluster.

# Source Code
<!-- Source code will be available soon. -->
The source code of ApproxIoT is available <a href="https://github.com/ApproxIoT/ApproxIoT/tree/master/code"> here </a>
<!-- * Cluster deployment <a href="https://github.com/streamapprox/flink-setup"> script </a> -->

<!-- * <a href="https://github.com/streamapprox/spark"> Spark-based implementation </a> -->
