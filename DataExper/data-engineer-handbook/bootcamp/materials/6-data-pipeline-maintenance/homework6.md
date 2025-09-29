# Week 5: Data Pipeline Maintenance

## Overview

This document outlines the assignment for managing the five data pipelines across three business areas: Profit, Growth, and Engagement. We are part of a data engineering team of 4 members, and for this assignment, we have detailed the following aspects:

1. **Pipeline Ownership**: Primary and secondary owners for each pipeline.
2. **On-Call Schedule**: A fair on-call schedule taking into account holidays.
3. **Run Books**: Documentation for run books of pipelines that report metrics to investors, along with potential failure modes.

---

## 1. Pipeline Ownership

We have 5 pipelines covering three business areas:

- **Profit**
  - Unit-level profit for experiments.
  - Aggregate profit reported to investors.
  
- **Growth**
  - Aggregate growth reported to investors.
  - Daily growth needed for experiments.
  
- **Engagement**
  - Aggregate engagement reported to investors.
  
### Assignment of Ownership

For clarity, let's assume our team members are: **Alice, Bob, Charlie, and Dana**.

| Pipeline                                    | Primary Owner | Secondary Owner  |
|---------------------------------------------|---------------|------------------|
| **Unit-level Profit for Experiments**       | Alice         | Bob              |
| **Aggregate Profit for Investors**          | Bob           | Charlie          |
| **Aggregate Growth for Investors**          | Charlie       | Dana             |
| **Daily Growth for Experiments**            | Dana          | Alice            |
| **Aggregate Engagement for Investors**      | Alice         | Charlie          |

*Note: These roles can be rotated quarterly to ensure fairness and skill growth for all team members.*

---

## 2. On-Call Schedule

A fair on-call schedule should distribute the responsibility equally among team members, and account for holidays and team members’ availability. Here’s an example schedule for a two-week rotation period:

### On-Call Rotation (2-Week Cycle)

| Week          | On-Call Engineer | Backup Engineer(s)       |
|---------------|------------------|--------------------------|
| Week 1        | Alice            | Bob, Charlie, Dana       |
| Week 2        | Bob              | Alice, Charlie, Dana     |
| Week 3        | Charlie          | Alice, Bob, Dana         |
| Week 4        | Dana             | Alice, Bob, Charlie      |

#### Considerations for Holidays:
- **Holiday Coverage:** 
  - Maintain a separate rotation for holiday periods. If a holiday falls within a week, the on-call engineer can switch with the backup or designated holiday on-call, ensuring continuous coverage.
  - Example: For major holidays (e.g., New Year’s, Independence Day), a pre-scheduled swap is agreed upon at the start of the year.
  
- **Communication:** 
  - Team calendar integration (e.g., using Google Calendar or Outlook) to mark holidays and on-call shifts.
  - Proactive communication a week in advance for planned holiday swaps.

---

## 3. Run Books for Pipelines Reporting Metrics to Investors

The following run books cover the **Aggregate Profit for Investors**, **Aggregate Growth for Investors**, and **Aggregate Engagement for Investors** pipelines. Each run book includes common potential issues, although for this exercise we do not recommend immediate course-of-action responses.

### Run Book Template

#### Pipeline Name:
*e.g., Aggregate Profit for Investors*

#### Primary Owner:
*e.g., Bob (with Charlie as the secondary owner)*

#### Description:
A brief description of what the pipeline does, its data sources, processing steps, and the format of the reported metrics.

#### Potential Failure Modes and What Could Go Wrong:

1. **Data Source Unavailability**
   - **Symptoms:** Missing data, long processing times, or pipeline failures.
   - **Potential Causes:** External API downtime, database connectivity issues.

2. **Data Quality Issues**
   - **Symptoms:** Incorrect metrics, anomalies in reports.
   - **Potential Causes:** Inconsistent data formats, delayed or corrupt data ingestion.

3. **Pipeline Processing Failures**
   - **Symptoms:** Job crashes, error logs, incomplete data transformations.
   - **Potential Causes:** Code errors, version mismatches, dependency issues.

4. **Infrastructure Problems**
   - **Symptoms:** Slow performance, timeouts, or no results.
   - **Potential Causes:** Server downtime, network issues, configuration errors in cloud services.

5. **Changes in Data Schemas**
   - **Symptoms:** Failures in data ingestion or transformation routines.
   - **Potential Causes:** Upstream schema changes that are not communicated or integrated into pipeline logic.

6. **Security or Access Issues**
   - **Symptoms:** Unauthorized data access attempts, failed authentications.
   - **Potential Causes:** Token expiries, misconfigured permissions, security policy changes.

#### Monitoring and Alerts:
- Pipeline logs (integrated with the logging system).
- Alerts for non-completion, data lag or quality thresholds.
- Automated tests in the CI/CD process to catch schema and data changes in advance.

#### Recovery / Next Steps:
- *The run book does not define recovery steps in this exercise, but a real-world scenario should include troubleshooting steps and a rollback plan.*

### Example Run Book Entry

```markdown
#### Pipeline: Aggregate Profit for Investors

**Primary Owner:** Bob  
**Secondary Owner:** Charlie

**Description:**
This pipeline aggregates unit-level profits, cleans the data, transforms it and then aggregates the final profit report for investors. It integrates data from operational systems and external financial APIs.

**Potential Failure Modes:**
1. **Data Source Unavailability**: An external financial API is down, leading to missing data.
2. **Data Quality Issues**: Corrupt data entries due to unexpected changes in source data format.
3. **Pipeline Processing Failures**: Code exceptions due to version mismatches in the transformation libraries.
4. **Infrastructure Problems**: Cloud server outages causing timeouts or failed jobs.
5. **Schema Changes**: Upstream changes in source data leading to field mismatches.
6. **Security or Access Issues**: Revoked API keys or changed permissions on the financial data endpoint.

**Monitoring and Alerts:**
- Real-time dashboard monitoring for pipeline latency and error rates.
- Automated alerts triggered by data quality tests.
- Scheduled health checks during peak business hours.

---

This Markdown file provides a structured approach to managing data pipelines, assigning ownership, creating an on-call schedule, and drafting run books with potential failure modes. Make sure to adjust and expand on the details based on team feedback and operational specifics.