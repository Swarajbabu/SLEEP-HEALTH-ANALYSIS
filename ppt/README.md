# 📊 Academic Project Presentation: Sleep Health Analysis & Risk Prediction Using Apache PySpark

<p align="center">
  <img src="../images/lpu_logo.png" width="300" alt="Lovely Professional University" /><br/>
  <b>Course Code</b>: INT315 / NT315 : Cluster Computing &bull; <b>Academic Assessment</b>: CA1 Project Defense<br/>
  <b>Student</b>: Vecha Laxmi Swaraj Babu (Registration: <code>12307477</code> | Roll: <code>45</code>)<br/>
  <b>Faculty Supervisor</b>: Dr. / Prof. Pardeep Kumar &bull; <b>Institution</b>: School of Computer Science, LPU
</p>

---

## 📥 Direct Download

- **PowerPoint File (.pptx)**: [Download `Sleep_Health_Cluster_Computing_Presentation.pptx`](./Sleep_Health_Cluster_Computing_Presentation.pptx)
- **Format**: 16:9 Modern Widescreen (1920 × 1080)
- **Theme**: Executive Midnight Slate & Tech Cyan/Orange Accents (Non-AI, Senior Architect Style)
- **Branding**: Official Lovely Professional University logo branded on every slide

---

## 🧭 Presentation Structure & Slide-by-Slide Previews

```
Total Presentation Time: 8 to 10 Minutes (~40–55 seconds per slide)
Total Slides: 13 Slides
```

### Slide 01: Title Slide & Academic Defense Cover
<p align="center">
  <img src="slides/Slide1.PNG" width="850" alt="Slide 1: Title Slide" />
</p>

- **Category**: `LOVELY PROFESSIONAL UNIVERSITY | INT315 / NT315`
- **Key Visuals**: Official LPU Logo pill, student/supervisor metadata card, big KPI badges (100k records, 82.43% accuracy, 81.32% F1, `local[*]`).
- **Verbatim Speaker Script** *(40 sec)*:
  > *"Respected faculty guide Dr. Pardeep Kumar and committee members, good morning. I am Vecha Laxmi Swaraj Babu, registration number 12307477, roll number 45. Today, I am presenting my CA1 Project for INT315 Cluster Computing: 'Sleep Health Analysis and Disorder Risk Prediction Using Apache PySpark'. In this project, we designed and implemented a distributed, multi-class machine learning workflow on Apache Spark. We processed 100,000 patient observations across 32 clinical features, implemented leakage-free feature pipelines, and trained a distributed Random Forest model achieving 82.43% test accuracy on 20,000 unseen patients. All artifacts are live on GitHub under Swarajbabu/SLEEP-HEALTH-ANALYSIS."*

---

### Slide 02: Clinical Dilemma vs. Computational Memory Wall
<p align="center">
  <img src="slides/Slide2.PNG" width="850" alt="Slide 2: Clinical Problem & Big Data Crisis" />
</p>

- **Category**: `PROBLEM STATEMENT & MOTIVATION`
- **Key Visuals**: Split-screen comparing Polysomnography (PSG) limits with Single-Node Python RAM limits, leading into the Spark distributed solution.
- **Verbatim Speaker Script** *(50 sec)*:
  > *"To understand why cluster computing is essential here, we must recognize two bottlenecks. First, clinically: sleep disorders affect over 30% of adults, but the gold-standard diagnostic tool—Polysomnography—requires an overnight stay in a hospital with dozens of wired sensors. It is invasive, expensive, and backlogged by months. It cannot scale for preventative screening. Second, computationally: smartwatches and EHRs generate massive continuous telemetry. If you attempt to load 100,000 records with 32 features into standard single-node tools like Pandas or Scikit-Learn, one-hot encoding and matrix operations exhaust host RAM, causing disk thrashing and crashes. Apache Spark solves this dual crisis by partitioning data across multi-threaded CPU worker cores."*

---

### Slide 03: Core Objectives of the Distributed Workflow
<p align="center">
  <img src="slides/Slide3.PNG" width="850" alt="Slide 3: Project Objectives & Scope" />
</p>

- **Category**: `PROJECT OBJECTIVES & SCOPE`
- **Key Visuals**: 4 sequential numbered objective badges (01 Ingestion, 02 Audit & EDA, 03 Leakage Sanitization, 04 Ensemble Training & Caching).
- **Verbatim Speaker Script** *(45 sec)*:
  > *"We organized the project around four technical milestones: First, distributed ingestion of 100,000 records without schema truncation in local[*] master mode. Second, parallel quality auditing across all 32 columns at once using Spark SQL aggregations. Third, target leakage prevention—dropping retrospective recovery indicators so the model can be used for prospective screening—and assembling a unified PySpark MLlib Pipeline. Fourth, distributed ensemble model fitting, in-memory inference caching, and quantitative multiclass evaluation."*

---

### Slide 04: 100,000-Patient Cohort & Feature Dimension Taxonomy
<p align="center">
  <img src="slides/Slide4.PNG" width="850" alt="Slide 4: Dataset Taxonomy & Cohort Profile" />
</p>

- **Category**: `DATASET SPECIFICATION & CLINICAL ATTRIBUTES`
- **Key Visuals**: Top metric pills (100k, 32 attrs, 14.87 MB, CDC NHIS calibrated), 5-domain clinical feature breakdown, and target class distribution table.
- **Verbatim Speaker Script** *(50 sec)*:
  > *"Our dataset contains 100,000 observations modeled on CDC NHIS benchmarks, spanning Demographics, Sleep Architecture, Autonomic Vitals, Lifestyle, and Circadian Dynamics. Please direct your attention to the target distribution on the right: Healthy accounts for 54.16%, Mild for 33.48%, Moderate for 8.30%, and Severe for 4.07%. Notice that Healthy and Mild make up over 87% of the cohort. This class imbalance is critical: a naive baseline that predicts 'Healthy' every time achieves 54.16% accuracy while having zero clinical diagnostic utility. Our model must prove multi-class discriminative precision across all 4 tiers."*

---

### Slide 05: End-to-End Distributed Cluster Computing ML Workflow
<p align="center">
  <img src="slides/Slide5.PNG" width="850" alt="Slide 5: Distributed System Architecture" />
</p>

- **Category**: `DISTRIBUTED SYSTEMS ARCHITECTURE`
- **Key Visuals**: 5-stage sequential architecture cards (Ingestion $\rightarrow$ Audit $\rightarrow$ Leakage $\rightarrow$ Pipeline $\rightarrow$ Cached Evaluation), plus bottom engine principles card.
- **Verbatim Speaker Script** *(60 sec)*:
  > *"Here is our end-to-end distributed architecture. What makes PySpark superior to single-node Python are two distributed computing principles: Catalyst Query Optimization and in-memory caching. Catalyst defers physical processing until terminal actions like show() or count() occur, combining column projections and filter pushdowns to minimize off-heap memory footprints. Furthermore, when evaluating four multiclass metrics sequentially, an un-cached DataFrame forces Spark to re-execute the upstream 5-stage feature transformation pipeline four separate times. By explicitly invoking predictions.cache() in executor RAM, we eliminate 3 redundant pipeline executions."*

---

### Slide 06: Parallel Null Verification & Risk Stratification Analysis
<p align="center">
  <img src="slides/Slide6.PNG" width="850" alt="Slide 6: Data Quality & EDA Verification" />
</p>

- **Category**: `DATA QUALITY & EXPLORATORY DATA ANALYSIS`
- **Key Visuals**: Parallel Spark SQL null verification code + console screenshot; Risk category distribution bar chart screenshot.
- **Verbatim Speaker Script** *(45 sec)*:
  > *"Slide 6 presents our actual notebook execution. On the left is our distributed missing value audit. Rather than looping through columns sequentially in Python, we constructed a single Spark SQL select expression containing 32 conditional aggregations. Spark distributes this query across all partition blocks, verifying in parallel that zero null values exist across the entire 100,000-record dataset. On the right is the class skew visualization showing the steep gradient between healthy individuals and the minority severe risk tier."*

---

### Slide 07: Progressive Deterioration of Biomarkers Across Risk Tiers
<p align="center">
  <img src="slides/Slide7.PNG" width="850" alt="Slide 7: Clinical Biomarker Discovery" />
</p>

- **Category**: `CLINICAL BIOMARKER DISCOVERY`
- **Key Visuals**: 4 empirical project plots comparing Healthy vs. Mild vs. Moderate vs. Severe across Duration, Quality, Stress, and Cognition.
- **Verbatim Speaker Script** *(55 sec)*:
  > *"By running distributed group-by aggregations in PySpark, we discovered four distinct physiological trends. Nightly sleep duration decreases from 6.93 hours to 4.84 hours—a 30.2% drop. Sleep quality degrades sharply by 60.3%, from 5.77 down to 2.29. Daily stress rises from 5.85 to 7.48. But most strikingly, daytime cognitive performance collapses by 73.7%, falling from 72.35 to 19.04. This is a critical medical finding: cognitive decline (-73.7%) and quality loss (-60.3%) are more than double the rate of sleep duration loss (-30.2%), demonstrating that broken sleep architecture impairs executive neurocognitive functions far beyond mere sleep deprivation."*

---

### Slide 08: Prospective Triage Integrity & PySpark MLlib Pipeline
<p align="center">
  <img src="slides/Slide8.PNG" width="850" alt="Slide 8: Feature Engineering & Preprocessing" />
</p>

- **Category**: `FEATURE ENGINEERING & PREPROCESSING`
- **Key Visuals**: Target leakage explanation on left; MLlib Pipeline stages and code screenshot on right.
- **Verbatim Speaker Script** *(50 sec)*:
  > *"Slide 8 highlights our data engineering rigor: preventing target leakage. In medical machine learning, models frequently achieve fake 99% accuracy because retrospective symptom indicators are inadvertently fed into training. In this dataset, felt_rested and cognitive_performance_score are recorded the morning after sleep. If a patient feels unrefreshed or has a collapsed cognitive score, they already have active symptoms of the disorder! Including them creates retrospective target leakage. In prospective triage, you only have pre-sleep baseline habits and overnight vitals. We stripped these columns along with person_id and fed the remaining 28 features into our StringIndexer, OneHotEncoder, and VectorAssembler pipeline."*

---

### Slide 09: Distributed Ensemble Learning via Spark MLlib
<p align="center">
  <img src="slides/Slide9.PNG" width="850" alt="Slide 9: Model Training & Inference Dynamics" />
</p>

- **Category**: `MODEL TRAINING & INFERENCE DYNAMICS`
- **Key Visuals**: Why Random Forest on Spark; 50 trees, maxDepth 8 configuration; Model fit and test predictions preview screenshot.
- **Verbatim Speaker Script** *(45 sec)*:
  > *"Why did we choose RandomForestClassifier in Spark MLlib? First, clinical biomarker interactions are highly non-linear—for example, high stress combined with high evening blue-light screen exposure triggers disproportionate risk. Decision trees capture non-linear feature splits without requiring min-max scaling. Second, Random Forest is embarrassingly parallel: each decision tree is grown independently. Spark's task scheduler distributes tree-building tasks across all CPU worker threads concurrently. We trained 50 trees with a maximum depth of 8 on 80,000 records and generated predictions on 20,000 unseen hold-out patients."*

---

### Slide 10: Multi-Metric Model Validation on 20,000 Unseen Patients
<p align="center">
  <img src="slides/Slide10.PNG" width="850" alt="Slide 10: Results & Quantitative Evaluation" />
</p>

- **Category**: `RESULTS & QUANTITATIVE EVALUATION`
- **Key Visuals**: 4 large KPI cards (Accuracy: 82.43%, F1: 81.32%, Precision: 80.35%, Recall: 82.43%); Spark evaluation console screenshot; Performance bar chart screenshot.
- **Verbatim Speaker Script** *(50 sec)*:
  > *"Here are our primary empirical results evaluated on the 20,000 unseen hold-out test set using PySpark's MulticlassClassificationEvaluator. Overall Accuracy reached 82.43%. Weighted F1-Score reached 81.32%. Weighted Precision reached 80.35%, and Weighted Recall reached 82.43%. Our model outperforms the naive majority baseline of 54.16% by more than 28 percentage points. The close balance between precision (80.35%) and recall (82.43%) proves that our ensemble minimizes false alarms while maintaining high detection sensitivity for high-risk patients."*

---

### Slide 11: Spark Computational Advantages: Catalyst, Caching & Parallelism
<p align="center">
  <img src="slides/Slide11.PNG" width="850" alt="Slide 11: Cluster Computing Architecture" />
</p>

- **Category**: `CLUSTER COMPUTING ARCHITECTURE`
- **Key Visuals**: 4 technical cluster computing pillars (Catalyst, `.cache()`, Multi-core scheduling, Enterprise cluster migration).
- **Verbatim Speaker Script** *(50 sec)*:
  > *"As a core deliverable for INT315 Cluster Computing, Slide 11 summarizes the three computational advantages of Apache Spark over single-node systems. First, Catalyst query optimization constructs an optimized logical plan, pushing down filters and column projections to minimize heap memory usage. Second, in-memory caching: calling predictions.cache() stored the inference DataFrame in executor RAM, eliminating redundant 5-stage pipeline re-evaluations during our metric passes. Third, multi-threaded CPU parallelization in local[*] mode. Finally, because our code uses standard PySpark APIs, this entire pipeline can be deployed directly to an enterprise Hadoop YARN, AWS EMR, or Kubernetes cluster without rewriting any business logic."*

---

### Slide 12: Engineering Trade-Offs & Strategic Scalability Roadmap
<p align="center">
  <img src="slides/Slide12.PNG" width="850" alt="Slide 12: Limitations & Future Roadmap" />
</p>

- **Category**: `LIMITATIONS & FUTURE ROADMAP`
- **Key Visuals**: Left: Recognized project limitations; Right: Strategic future roadmap (Grid search, cost-sensitive rebalancing, Spark Streaming).
- **Verbatim Speaker Script** *(45 sec)*:
  > *"A mature engineering presentation acknowledges limitations and defines a future roadmap. On the left, we note three trade-offs: the dataset is synthetically calibrated; the cluster was run in local[*] virtual mode rather than across a physical multi-rack network; and hyperparameters were manually fixed. On the right is our roadmap: first, distributed hyperparameter tuning using ParamGridBuilder and CrossValidator; second, class-weighted loss to increase detection on the rare 4.07% severe cohort; and third, extending batch processing into real-time streaming via Spark Structured Streaming and Apache Kafka to score wearable telemetry as patients sleep."*

---

### Slide 13: Project Synthesis, Repository Deliverables & Viva Voce
<p align="center">
  <img src="slides/Slide13.PNG" width="850" alt="Slide 13: Project Summary & Defense" />
</p>

- **Category**: `PROJECT SUMMARY & DEFENSE`
- **Key Visuals**: Summary of milestones, verified repository deliverables checklist, and closing defense banner.
- **Verbatim Speaker Script** *(40 sec)*:
  > *"In summary, we have demonstrated that Apache PySpark provides a robust, scalable cluster computing platform for clinical sleep disorder prediction, delivering 82.43% test accuracy while preventing single-node memory failure and clinical target leakage. All code, datasets, visual figures, and documentation are live and verified on GitHub under Swarajbabu/SLEEP-HEALTH-ANALYSIS. Thank you, Dr. Pardeep Kumar and respected panel members. I am now open for questions, feedback, and defense."*

---

## 🎯 Viva Voce & Defense Cheat Sheet (Top 10 Questions)

1. **Why `local[*]`?**  
   *Answer*: It binds the Spark driver and executor to all available host vCPU cores. On a 4-core/8-thread machine, it spawns 8 concurrent worker threads, maximizing hardware utilization.
2. **What is Catalyst Query Optimization?**  
   *Answer*: Spark builds an Abstract Syntax Tree (AST) and optimizes logical plans before physical execution, applying filter pushdown and column projection to eliminate off-heap memory waste.
3. **Why call `predictions.cache()`?**  
   *Answer*: We ran 4 separate evaluations (Accuracy, F1, Precision, Recall). Without `.cache()`, Spark evaluates the upstream 5-stage transformation DAG 4 times. Caching persists predictions in executor RAM, eliminating redundant passes.
4. **Why drop `felt_rested` and `cognitive_performance_score`?**  
   *Answer*: They are retrospective next-day outcomes. Including them causes target leakage (symptoms predicting disease) and produces inflated ~99% accuracy that fails in actual prospective clinical triage.
5. **Why is unweighted accuracy misleading?**  
   *Answer*: Healthy (54.16%) + Mild (33.48%) make up 87.64% of cases. A naive baseline guessing Healthy gets 54.16% accuracy. Weighted F1 (81.32%) ensures minority cohorts like Severe (4.07%) are actively evaluated.
6. **Why Random Forest over Logistic Regression?**  
   *Answer*: Biomarkers interact non-linearly (stress + sleep + screen time). Decision trees split feature spaces non-linearly without scaling, and tree building is embarrassingly parallel across cores.
7. **What do `StringIndexer`, `OneHotEncoder`, and `VectorAssembler` do?**  
   *Answer*: `StringIndexer` converts strings to integer indices; `OneHotEncoder` creates sparse binary dummy vectors; `VectorAssembler` merges all 28 features into a single dense vector column.
8. **How do you scale to AWS EMR or YARN?**  
   *Answer*: Change master URL to `yarn` or `spark://`, submit via `spark-submit --deploy-mode cluster`, and point input path to an Amazon S3 or HDFS URI. Zero code changes required.
9. **Why 80/20 train/test split?**  
   *Answer*: On 100,000 records, an 80/20 split provides 80,000 training records and 20,000 test records, which is statistically robust for multi-class hold-out evaluation.
10. **How would you support real-time smartwatch streams?**  
    *Answer*: Transition to Spark Structured Streaming and Apache Kafka: devices publish JSON telemetry to Kafka, and Spark ingests micro-batches through `model.transform()` for live scoring.
