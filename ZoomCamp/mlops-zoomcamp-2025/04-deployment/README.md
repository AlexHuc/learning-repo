# Experiment Tracking

```mermaid
stateDiagram
    Design
    Train
    Operate
```
Schema:

```mermaid
graph LR
A(Training Pipeline) --> B[model] --> C(Deployment)
```

## Deployment

```mermaid
stateDiagram
    Deployment --> BatchOffline : Run Regularly
    Deployment --> Online : Running all the time

    Online --> WebService
    Online --> Streaming
```

### Batch Offline

- Run the model regularly (hourly, daily, monthly)

```mermaid
graph LR
A(DB) --> |Get 
all 
data 
from 
yesterday| B[Scoring Job model] --> C(Predictions) --> D(Show predictions)
```

#### Marketing

- `Churn` - decide to stop using the service from a company and go use the service of a competitor

![Churn](/04-deployment/imgs/Chrun.png)

### Web Service

![WebService](/04-deployment/imgs/WebService.png)

### Streaming

![WebService](/04-deployment/imgs/Streaming1.png)
![WebService](/04-deployment/imgs/Streaming2.png)