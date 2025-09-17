# Data Collaboration Studio - Technical Implementation Guide

## 1. Microservices Implementation Details

### Service Architecture Template

```golang
// Base service structure for all microservices
package main

import (
    "context"
    "github.com/go-kit/kit/endpoint"
    "github.com/go-kit/kit/log"
    "github.com/go-kit/kit/metrics"
    "github.com/go-kit/kit/tracing/opentracing"
    "github.com/uber/jaeger-client-go"
)

type BaseService struct {
    logger    log.Logger
    metrics   *ServiceMetrics
    tracer    opentracing.Tracer
    config    *ServiceConfig
    healthy   bool
    ready     bool
}

type ServiceMetrics struct {
    RequestCount   metrics.Counter
    RequestLatency metrics.Histogram
    ErrorCount     metrics.Counter
}

// Middleware chain for all endpoints
func MakeEndpointMiddleware(s BaseService) endpoint.Middleware {
    return endpoint.Chain(
        RateLimitMiddleware(1000), // 1000 req/sec
        AuthenticationMiddleware(s.config.AuthConfig),
        AuthorizationMiddleware(s.config.RBACConfig),
        TracingMiddleware(s.tracer),
        MetricsMiddleware(s.metrics),
        CircuitBreakerMiddleware(s.config.CircuitBreaker),
        RetryMiddleware(3, time.Second),
        TimeoutMiddleware(30 * time.Second),
    )
}

// Health check implementation
func (s *BaseService) HealthCheck(ctx context.Context) HealthStatus {
    checks := []HealthCheck{
        s.checkDatabase(),
        s.checkCache(),
        s.checkMessageBroker(),
        s.checkDependencies(),
    }
    
    status := HealthStatus{
        Service:   s.config.ServiceName,
        Timestamp: time.Now(),
        Checks:    checks,
    }
    
    for _, check := range checks {
        if !check.Healthy {
            status.Status = "unhealthy"
            s.healthy = false
            return status
        }
    }
    
    status.Status = "healthy"
    s.healthy = true
    return status
}
```

### Data Ingestion Service

```python
from typing import AsyncIterator, Optional
import asyncio
from dataclasses import dataclass
import aioboto3
import aioredis
from motor.motor_asyncio import AsyncIOMotorClient

@dataclass
class IngestionConfig:
    max_file_size: int = 10 * 1024 * 1024 * 1024  # 10GB
    supported_formats: list = None
    chunk_size: int = 1024 * 1024  # 1MB chunks
    parallel_uploads: int = 10
    virus_scan_enabled: bool = True

class DataIngestionService:
    def __init__(self, config: IngestionConfig):
        self.config = config
        self.s3_client = None
        self.redis_client = None
        self.mongo_client = None
        self.kafka_producer = None
        
    async def initialize(self):
        """Initialize all connections"""
        session = aioboto3.Session()
        self.s3_client = await session.client('s3').__aenter__()
        self.redis_client = await aioredis.create_redis_pool(
            'redis://localhost',
            minsize=5,
            maxsize=10
        )
        self.mongo_client = AsyncIOMotorClient('mongodb://localhost:27017')
        self.kafka_producer = await self._init_kafka_producer()
    
    async def ingest_file(
        self,
        file_stream: AsyncIterator[bytes],
        metadata: dict,
        tenant_id: str
    ) -> str:
        """Main ingestion pipeline"""
        
        # Step 1: Validate file
        validation_result = await self._validate_file(file_stream, metadata)
        if not validation_result.is_valid:
            raise ValidationError(validation_result.errors)
        
        # Step 2: Virus scan
        if self.config.virus_scan_enabled:
            scan_result = await self._scan_for_virus(file_stream)
            if scan_result.infected:
                raise SecurityError(f"Virus detected: {scan_result.virus_name}")
        
        # Step 3: Calculate checksums
        checksums = await self._calculate_checksums(file_stream)
        
        # Step 4: Check for duplicates
        if await self._is_duplicate(checksums.sha256, tenant_id):
            existing_id = await self._get_existing_dataset_id(checksums.sha256)
            return existing_id
        
        # Step 5: Upload to S3 with multipart upload
        s3_location = await self._multipart_upload(
            file_stream,
            tenant_id,
            metadata
        )
        
        # Step 6: Create metadata record
        dataset_id = await self._create_metadata_record(
            s3_location,
            metadata,
            checksums,
            tenant_id
        )
        
        # Step 7: Emit ingestion event
        await self._emit_event(
            'dataset.ingested',
            {
                'dataset_id': dataset_id,
                'tenant_id': tenant_id,
                'size': metadata['size'],
                'format': metadata['format']
            }
        )
        
        return dataset_id
    
    async def _multipart_upload(
        self,
        file_stream: AsyncIterator[bytes],
        tenant_id: str,
        metadata: dict
    ) -> str:
        """Multipart upload to S3 with progress tracking"""
        
        bucket = f"dcs-{tenant_id}"
        key = f"raw/{datetime.utcnow().isoformat()}/{metadata['filename']}"
        
        # Initiate multipart upload
        response = await self.s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=key,
            ServerSideEncryption='aws:kms',
            Metadata=metadata
        )
        upload_id = response['UploadId']
        
        parts = []
        part_number = 1
        
        try:
            # Upload parts in parallel
            async for chunk in self._read_chunks(file_stream):
                part_response = await self.s3_client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )
                
                parts.append({
                    'ETag': part_response['ETag'],
                    'PartNumber': part_number
                })
                
                # Update progress in Redis
                await self._update_progress(
                    upload_id,
                    part_number,
                    len(chunk)
                )
                
                part_number += 1
            
            # Complete multipart upload
            await self.s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            return f"s3://{bucket}/{key}"
            
        except Exception as e:
            # Abort upload on error
            await self.s3_client.abort_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id
            )
            raise IngestionError(f"Upload failed: {str(e)}")
    
    async def _read_chunks(
        self,
        file_stream: AsyncIterator[bytes]
    ) -> AsyncIterator[bytes]:
        """Read file in chunks for multipart upload"""
        
        buffer = b""
        async for data in file_stream:
            buffer += data
            
            while len(buffer) >= self.config.chunk_size:
                yield buffer[:self.config.chunk_size]
                buffer = buffer[self.config.chunk_size:]
        
        if buffer:
            yield buffer
```

### Validation Service with PII Detection

```python
import spacy
from presidio_analyzer import AnalyzerEngine
from typing import List, Dict, Any
import pandas as pd
import great_expectations as ge

class DataValidationService:
    def __init__(self):
        self.analyzer = AnalyzerEngine()
        self.nlp = spacy.load("en_core_web_lg")
        self.ge_context = ge.data_context.DataContext()
        
    async def validate_dataset(
        self,
        dataset_path: str,
        schema: Dict[str, Any],
        validation_rules: List[ValidationRule]
    ) -> ValidationResult:
        """Comprehensive dataset validation"""
        
        results = ValidationResult()
        
        # Load dataset
        df = await self._load_dataset(dataset_path)
        
        # Schema validation
        schema_errors = await self._validate_schema(df, schema)
        results.add_errors('schema', schema_errors)
        
        # PII detection
        pii_findings = await self._detect_pii(df)
        results.add_findings('pii', pii_findings)
        
        # Data quality checks
        quality_issues = await self._check_data_quality(df, validation_rules)
        results.add_issues('quality', quality_issues)
        
        # Statistical anomalies
        anomalies = await self._detect_anomalies(df)
        results.add_anomalies('statistical', anomalies)
        
        # Business rule validation
        rule_violations = await self._validate_business_rules(df, validation_rules)
        results.add_violations('business', rule_violations)
        
        return results
    
    async def _detect_pii(self, df: pd.DataFrame) -> List[PIIFinding]:
        """Detect PII using Presidio"""
        
        findings = []
        
        for column in df.columns:
            sample_data = df[column].dropna().astype(str).head(1000)
            
            for idx, text in enumerate(sample_data):
                # Analyze text for PII
                results = self.analyzer.analyze(
                    text=text,
                    language='en',
                    entities=[
                        "PHONE_NUMBER",
                        "EMAIL_ADDRESS", 
                        "CREDIT_CARD",
                        "US_SSN",
                        "PERSON",
                        "LOCATION",
                        "IP_ADDRESS",
                        "MEDICAL_LICENSE",
                        "US_DRIVER_LICENSE"
                    ]
                )
                
                if results:
                    findings.append(PIIFinding(
                        column=column,
                        row=idx,
                        entities=[r.entity_type for r in results],
                        confidence=max(r.score for r in results)
                    ))
        
        return findings
    
    async def _check_data_quality(
        self,
        df: pd.DataFrame,
        rules: List[ValidationRule]
    ) -> List[QualityIssue]:
        """Run Great Expectations data quality checks"""
        
        # Create Great Expectations dataset
        ge_df = ge.from_pandas(df)
        
        issues = []
        
        # Completeness checks
        for column in df.columns:
            null_percentage = df[column].isnull().sum() / len(df)
            if null_percentage > 0.05:  # More than 5% nulls
                issues.append(QualityIssue(
                    type='completeness',
                    column=column,
                    severity='warning',
                    message=f'{null_percentage:.2%} null values'
                ))
        
        # Uniqueness checks
        for column in df.select_dtypes(include=['object']).columns:
            unique_ratio = df[column].nunique() / len(df)
            if unique_ratio < 0.01:  # Less than 1% unique
                issues.append(QualityIssue(
                    type='uniqueness',
                    column=column,
                    severity='info',
                    message=f'Low cardinality: {unique_ratio:.2%} unique'
                ))
        
        # Format validation
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        phone_pattern = r'^\+?1?\d{9,15}$'
        
        for column in df.select_dtypes(include=['object']).columns:
            sample = df[column].dropna().head(100)
            
            # Check if column might contain emails
            if sample.str.contains('@').any():
                invalid_emails = ~sample.str.match(email_pattern)
                if invalid_emails.any():
                    issues.append(QualityIssue(
                        type='format',
                        column=column,
                        severity='error',
                        message=f'{invalid_emails.sum()} invalid email formats'
                    ))
        
        return issues
    
    async def _detect_anomalies(self, df: pd.DataFrame) -> List[Anomaly]:
        """Statistical anomaly detection"""
        
        from scipy import stats
        anomalies = []
        
        # Detect outliers in numeric columns
        for column in df.select_dtypes(include=['float64', 'int64']).columns:
            # Z-score method
            z_scores = np.abs(stats.zscore(df[column].dropna()))
            outliers = z_scores > 3
            
            if outliers.any():
                anomalies.append(Anomaly(
                    type='outlier',
                    column=column,
                    count=outliers.sum(),
                    method='z-score',
                    threshold=3
                ))
            
            # IQR method
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            iqr_outliers = (df[column] < lower_bound) | (df[column] > upper_bound)
            
            if iqr_outliers.any():
                anomalies.append(Anomaly(
                    type='outlier',
                    column=column,
                    count=iqr_outliers.sum(),
                    method='iqr',
                    bounds=(lower_bound, upper_bound)
                ))
        
        return anomalies
```

### Workflow Engine Implementation

```typescript
// Temporal-based workflow engine
import { 
  WorkflowClient,
  WorkflowHandle,
  Connection,
  WorkflowExecutionInfo 
} from '@temporalio/client';
import { Worker } from '@temporalio/worker';
import { DataApprovalWorkflow } from './workflows';
import { activities } from './activities';

export class WorkflowEngine {
  private client: WorkflowClient;
  private worker: Worker;
  
  constructor(private config: WorkflowConfig) {}
  
  async initialize(): Promise<void> {
    // Connect to Temporal server
    const connection = await Connection.connect({
      address: this.config.temporalAddress,
      tls: this.config.tlsConfig
    });
    
    this.client = new WorkflowClient({
      connection,
      namespace: this.config.namespace,
    });
    
    // Create worker
    this.worker = await Worker.create({
      connection,
      namespace: this.config.namespace,
      taskQueue: 'data-approval-queue',
      workflows: [DataApprovalWorkflow],
      activities,
      maxConcurrentActivityTaskExecutions: 100,
      maxConcurrentWorkflowTaskExecutions: 50,
    });
  }
  
  async startApprovalWorkflow(
    request: ApprovalRequest
  ): Promise<WorkflowHandle> {
    const handle = await this.client.start(DataApprovalWorkflow, {
      taskQueue: 'data-approval-queue',
      workflowId: `approval-${request.datasetId}`,
      args: [request],
      workflowIdReusePolicy: 'REJECT_DUPLICATE',
      searchAttributes: {
        TenantId: [request.tenantId],
        DatasetId: [request.datasetId],
        RequesterId: [request.requesterId],
        Status: ['PENDING']
      }
    });
    
    return handle;
  }
  
  async queryWorkflowStatus(workflowId: string): Promise<WorkflowStatus> {
    const handle = this.client.getHandle(workflowId);
    const status = await handle.query<WorkflowStatus>('getStatus');
    return status;
  }
  
  async cancelWorkflow(workflowId: string, reason: string): Promise<void> {
    const handle = this.client.getHandle(workflowId);
    await handle.cancel(reason);
  }
  
  async getWorkflowHistory(workflowId: string): Promise<WorkflowExecutionInfo> {
    const handle = this.client.getHandle(workflowId);
    const info = await handle.describe();
    return info;
  }
}

// Complex approval workflow implementation
export async function DataApprovalWorkflow(
  request: ApprovalRequest
): Promise<ApprovalResult> {
  const logger = workflow.getLogger();
  let currentStatus: WorkflowStatus = {
    stage: 'INITIALIZING',
    progress: 0,
    details: {}
  };
  
  // Define workflow queries
  workflow.setHandler('getStatus', () => currentStatus);
  
  // Define workflow signals
  workflow.setHandler('expedite', () => {
    logger.info('Expediting approval process');
    currentStatus.expedited = true;
  });
  
  try {
    // Stage 1: Data validation
    currentStatus = { stage: 'VALIDATING', progress: 10 };
    const validationResult = await workflow.executeActivity(
      'validateDataset',
      request.datasetId,
      {
        startToCloseTimeout: '5 minutes',
        retry: {
          initialInterval: '1 second',
          backoffCoefficient: 2,
          maximumAttempts: 3
        }
      }
    );
    
    if (!validationResult.passed) {
      await workflow.executeActivity(
        'notifyRejection',
        request.requesterId,
        validationResult.errors
      );
      return { approved: false, reason: 'VALIDATION_FAILED' };
    }
    
    // Stage 2: Automated compliance checks
    currentStatus = { stage: 'COMPLIANCE_CHECK', progress: 30 };
    const complianceResult = await workflow.executeActivity(
      'checkCompliance',
      request.datasetId,
      {
        startToCloseTimeout: '10 minutes'
      }
    );
    
    // Stage 3: Human approvals (parallel)
    currentStatus = { stage: 'APPROVAL_PENDING', progress: 50 };
    
    const approvalPromises = [];
    
    // L1 approval
    if (request.approvalLevels.includes('L1')) {
      approvalPromises.push(
        workflow.executeActivity(
          'requestHumanApproval',
          {
            level: 'L1',
            datasetId: request.datasetId,
            timeout: currentStatus.expedited ? '1 hour' : '24 hours'
          }
        )
      );
    }
    
    // L2 approval (only for sensitive data)
    if (request.metadata.sensitivity === 'HIGH') {
      approvalPromises.push(
        workflow.executeActivity(
          'requestHumanApproval',
          {
            level: 'L2',
            datasetId: request.datasetId,
            timeout: currentStatus.expedited ? '2 hours' : '48 hours'
          }
        )
      );
    }
    
    const approvals = await Promise.all(approvalPromises);
    
    if (approvals.some(a => !a.approved)) {
      await workflow.executeActivity(
        'notifyRejection',
        request.requesterId,
        'Human review rejected'
      );
      return { approved: false, reason: 'HUMAN_REJECTION' };
    }
    
    // Stage 4: Data activation
    currentStatus = { stage: 'ACTIVATING', progress: 80 };
    await workflow.executeActivity(
      'activateDataset',
      request.datasetId,
      {
        startToCloseTimeout: '15 minutes'
      }
    );
    
    // Stage 5: Notification and cleanup
    currentStatus = { stage: 'COMPLETING', progress: 95 };
    await workflow.executeActivity(
      'notifyApproval',
      request.requesterId,
      request.datasetId
    );
    
    currentStatus = { stage: 'COMPLETED', progress: 100 };
    return { 
      approved: true, 
      datasetId: request.datasetId,
      activatedAt: new Date()
    };
    
  } catch (error) {
    logger.error('Workflow failed', { error });
    currentStatus = { 
      stage: 'FAILED', 
      progress: currentStatus.progress,
      error: error.message 
    };
    throw error;
  }
}
```

## 2. Infrastructure as Code

### Terraform Configuration

```hcl
# main.tf - Core infrastructure
terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }
  
  backend "s3" {
    bucket         = "dcs-terraform-state"
    key            = "infrastructure/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "dcs-terraform-locks"
    encrypt        = true
  }
}

# EKS Cluster
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 19.0"

  cluster_name    = "${var.environment}-dcs-cluster"
  cluster_version = "1.27"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  enable_irsa = true

  eks_managed_node_groups = {
    general = {
      desired_size = 3
      min_size     = 2
      max_size     = 10

      instance_types = ["m5.xlarge"]
      
      k8s_labels = {
        Environment = var.environment
        GithubRepo  = "data-collaboration-studio"
        GithubOrg   = "example-org"
      }
    }
    
    compute = {
      desired_size = 2
      min_size     = 1
      max_size     = 20

      instance_types = ["c5.2xlarge"]
      
      taints = [
        {
          key    = "compute"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      ]
      
      k8s_labels = {
        workload = "compute"
      }
    }
  }
}

# RDS Multi-AZ PostgreSQL
module "rds" {
  source  = "terraform-aws-modules/rds/aws"
  version = "~> 5.0"

  identifier = "${var.environment}-dcs-postgres"

  engine            = "postgres"
  engine_version    = "14.7"
  instance_class    = "db.r6g.xlarge"
  allocated_storage = 100
  storage_encrypted = true
  kms_key_id       = aws_kms_key.rds.arn

  db_name  = "dcs"
  username = "dcsadmin"
  port     = "5432"

  multi_az               = true
  db_subnet_group_name   = module.vpc.database_subnet_group
  vpc_security_group_ids = [module.security_group.security_group_id]

  maintenance_window = "Mon:00:00-Mon:03:00"
  backup_window      = "03:00-06:00"

  backup_retention_period = 30
  skip_final_snapshot    = false
  deletion_protection    = true

  performance_insights_enabled = true
  monitoring_interval         = "60"
  
  enabled_cloudwatch_logs_exports = ["postgresql"]
}

# S3 Buckets with versioning and encryption
resource "aws_s3_bucket" "data_lake" {
  for_each = toset(var.tenants)
  
  bucket = "${var.environment}-dcs-${each.key}"
  
  tags = {
    Environment = var.environment
    Tenant      = each.key
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  for_each = aws_s3_bucket.data_lake
  
  bucket = each.value.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  for_each = aws_s3_bucket.data_lake
  
  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# MSK Kafka Cluster
resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.environment}-dcs-kafka"
  kafka_version          = "3.4.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = module.vpc.private_subnets
    security_groups = [module.kafka_sg.security_group_id]
    
    storage_info {
      ebs_storage_info {
        volume_size = 1000
      }
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_id = aws_kms_key.kafka.arn
    
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.kafka.name
      }
    }
  }
}
```

### Kubernetes Manifests

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: dcs-production
  labels:
    name: dcs-production
    istio-injection: enabled

---
# configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: dcs-config
  namespace: dcs-production
data:
  database.yaml: |
    postgres:
      host: postgres.dcs-production.svc.cluster.local
      port: 5432
      database: dcs
      pool_size: 20
      max_overflow: 40
      pool_timeout: 30
      pool_recycle: 3600
    
    mongodb:
      uri: mongodb://mongo-0.mongo:27017,mongo-1.mongo:27017,mongo-2.mongo:27017
      database: dcs_metadata
      replica_set: rs0
      write_concern: majority
      read_preference: secondaryPreferred
    
    redis:
      nodes:
        - redis-0.redis:6379
        - redis-1.redis:6379
        - redis-2.redis:6379
      password: ${REDIS_PASSWORD}
      cluster_mode: true
      
  kafka.yaml: |
    brokers:
      - kafka-0.kafka:9092
      - kafka-1.kafka:9092
      - kafka-2.kafka:9092
    topics:
      events:
        name: dcs.events
        partitions: 50
        replication_factor: 3
      commands:
        name: dcs.commands
        partitions: 20
        replication_factor: 3
    consumer_groups:
      validators: dcs-validators
      processors: dcs-processors
      publishers: dcs-publishers

---
# secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: dcs-secrets
  namespace: dcs-production
type: Opaque
stringData:
  DATABASE_PASSWORD: ${DATABASE_PASSWORD}
  REDIS_PASSWORD: ${REDIS_PASSWORD}
  JWT_SECRET: ${JWT_SECRET}
  ENCRYPTION_KEY: ${ENCRYPTION_KEY}
  AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
  AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}

---
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ingestion-service
  namespace: dcs-production
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: ingestion-service
  template:
    metadata:
      labels:
        app: ingestion-service
        version: v1
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: ingestion-service
      containers:
      - name: ingestion-service
        image: dcs/ingestion-service:1.0.0
        ports:
        - containerPort: 8080
          name: http
        - containerPort: 9090
          name: metrics
        env:
        - name: ENVIRONMENT
          value: production
        - name: LOG_LEVEL
          value: info
        - name: DATABASE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: dcs-secrets
              key: DATABASE_PASSWORD
        envFrom:
        - configMapRef:
            name: dcs-config
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health/live
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
        volumeMounts:
        - name: config
          mountPath: /app/config
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: dcs-config

---
# hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ingestion-service-hpa
  namespace: dcs-production
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ingestion-service
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  - type: Pods
    pods:
      metric:
        name: pending_files
      target:
        type: AverageValue
        averageValue: "100"
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 100
        periodSeconds: 60
      - type: Pods
        value: 4
        periodSeconds: 60
      selectPolicy: Max

---
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: ingestion-service
  namespace: dcs-production
  labels:
    app: ingestion-service
spec:
  type: ClusterIP
  ports:
  - port: 80
    targetPort: 8080
    protocol: TCP
    name: http
  selector:
    app: ingestion-service

---
# istio-virtualservice.yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: ingestion-service
  namespace: dcs-production
spec:
  hosts:
  - ingestion.dcs.internal
  http:
  - match:
    - headers:
        x-version:
          exact: v2
    route:
    - destination:
        host: ingestion-service
        subset: v2
      weight: 20
    - destination:
        host: ingestion-service
        subset: v1
      weight: 80
  - route:
    - destination:
        host: ingestion-service
        subset: v1
    timeout: 30s
    retries:
      attempts: 3
      perTryTimeout: 10s
      retryOn: 5xx

---
# pdb.yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: ingestion-service-pdb
  namespace: dcs-production
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: ingestion-service
```

## 3. Security Implementation

### OAuth 2.0 / OIDC Integration

```javascript
// auth-service.js
const express = require('express');
const { Issuer, Strategy } = require('openid-client');
const passport = require('passport');
const jwt = require('jsonwebtoken');
const redis = require('redis');

class AuthenticationService {
  constructor(config) {
    this.config = config;
    this.redisClient = redis.createClient(config.redis);
    this.app = express();
    this.setupOIDC();
    this.setupMiddleware();
  }
  
  async setupOIDC() {
    // Discover OIDC provider
    const issuer = await Issuer.discover(this.config.oidc.discoveryUrl);
    
    const client = new issuer.Client({
      client_id: this.config.oidc.clientId,
      client_secret: this.config.oidc.clientSecret,
      redirect_uris: [this.config.oidc.redirectUri],
      response_types: ['code'],
    });
    
    // Configure passport strategy
    passport.use(
      'oidc',
      new Strategy(
        {
          client,
          params: {
            scope: 'openid profile email groups',
          },
        },
        async (tokenSet, userinfo, done) => {
          // Enrich user with additional claims
          const user = await this.enrichUserClaims(userinfo);
          
          // Generate internal JWT
          const internalToken = this.generateInternalToken(user);
          
          // Cache session
          await this.cacheSession(internalToken, user);
          
          return done(null, { user, token: internalToken });
        }
      )
    );
  }
  
  async enrichUserClaims(userinfo) {
    // Fetch additional user attributes from database
    const dbUser = await this.userRepository.findByEmail(userinfo.email);
    
    // Fetch user's roles and permissions
    const roles = await this.roleService.getUserRoles(dbUser.id);
    const permissions = await this.permissionService.getUserPermissions(dbUser.id);
    
    // Fetch user's tenant associations
    const tenants = await this.tenantService.getUserTenants(dbUser.id);
    
    return {
      id: dbUser.id,
      email: userinfo.email,
      name: userinfo.name,
      roles: roles,
      permissions: permissions,
      tenants: tenants,
      attributes: {
        department: dbUser.department,
        clearanceLevel: dbUser.clearanceLevel,
        dataClassifications: dbUser.dataClassifications,
      },
      metadata: {
        lastLogin: new Date(),
        loginCount: dbUser.loginCount + 1,
        ipAddress: this.getClientIP(),
      }
    };
  }
  
  generateInternalToken(user) {
    const payload = {
      sub: user.id,
      email: user.email,
      roles: user.roles,
      permissions: user.permissions,
      tenants: user.tenants,
      attributes: user.attributes,
      iat: Math.floor(Date.now() / 1000),
      exp: Math.floor(Date.now() / 1000) + (60 * 60 * 8), // 8 hours
      iss: 'dcs-auth-service',
      aud: 'dcs-services',
    };
    
    return jwt.sign(payload, this.config.jwt.secret, {
      algorithm: 'RS256',
      keyid: this.config.jwt.keyId,
    });
  }
  
  async validateToken(token) {
    try {
      // Check if token is blacklisted
      const isBlacklisted = await this.redisClient.get(`blacklist:${token}`);
      if (isBlacklisted) {
        throw new Error('Token has been revoked');
      }
      
      // Verify JWT signature
      const decoded = jwt.verify(token, this.config.jwt.publicKey, {
        algorithms: ['RS256'],
        issuer: 'dcs-auth-service',
        audience: 'dcs-services',
      });
      
      // Check if session is still valid
      const session = await this.redisClient.get(`session:${decoded.sub}`);
      if (!session) {
        throw new Error('Session expired');
      }
      
      // Refresh session TTL
      await this.redisClient.expire(`session:${decoded.sub}`, 3600);
      
      return decoded;
    } catch (error) {
      throw new Error(`Token validation failed: ${error.message}`);
    }
  }
  
  setupMiddleware() {
    // Rate limiting per user
    this.app.use(async (req, res, next) => {
      const userId = req.user?.id || req.ip;
      const key = `rate:${userId}`;
      
      const current = await this.redisClient.incr(key);
      if (current === 1) {
        await this.redisClient.expire(key, 60); // 1 minute window
      }
      
      if (current > this.config.rateLimit.maxRequests) {
        return res.status(429).json({
          error: 'Rate limit exceeded',
          retryAfter: 60
        });
      }
      
      next();
    });
    
    // Audit logging
    this.app.use((req, res, next) => {
      const auditLog = {
        timestamp: new Date(),
        userId: req.user?.id,
        method: req.method,
        path: req.path,
        ip: req.ip,
        userAgent: req.headers['user-agent'],
      };
      
      this.auditService.log(auditLog);
      next();
    });
  }
}
```

### Policy Engine (Open Policy Agent)

```rego
# policy.rego - Fine-grained authorization policies
package dcs.authz

import future.keywords.contains
import future.keywords.if
import future.keywords.in

# Default deny
default allow := false

# Allow if user has required permission
allow if {
    required_permission := data.permissions[input.resource][input.action]
    required_permission in input.user.permissions
}

# RBAC: Allow if user's role has permission
allow if {
    some role in input.user.roles
    required_permission := data.permissions[input.resource][input.action]
    required_permission in data.roles[role].permissions
}

# ABAC: Attribute-based access control
allow if {
    input.resource == "dataset"
    input.action == "read"
    input.resource_attributes.classification == "PUBLIC"
}

allow if {
    input.resource == "dataset"
    input.action == "read"
    input.resource_attributes.classification == "INTERNAL"
    input.user.attributes.employee == true
}

allow if {
    input.resource == "dataset"
    input.action == "read"
    input.resource_attributes.classification == "CONFIDENTIAL"
    input.user.attributes.clearanceLevel >= 2
    input.resource_attributes.department == input.user.attributes.department
}

allow if {
    input.resource == "dataset"
    input.action in ["update", "delete"]
    input.resource_attributes.ownerId == input.user.id
}

# Tenant isolation
allow if {
    input.resource_attributes.tenantId == input.user.tenantId
}

# Time-based access control
allow if {
    input.resource == "dataset"
    input.action == "export"
    current_time := time.now_ns() / 1000000000
    working_hours_start := time.parse_rfc3339_ns("2024-01-01T09:00:00Z") / 1000000000
    working_hours_end := time.parse_rfc3339_ns("2024-01-01T18:00:00Z") / 1000000000
    hour := time.clock(current_time)[0]
    hour >= 9
    hour <= 18
}

# Compliance rules
deny_reason contains msg if {
    input.resource == "dataset"
    input.action == "export"
    input.resource_attributes.containsPII == true
    not input.user.attributes.gdprTrainingCompleted
    msg := "User must complete GDPR training before exporting PII data"
}

deny_reason contains msg if {
    input.resource == "dataset"
    input.action == "share_external"
    input.resource_attributes.classification == "CONFIDENTIAL"
    not input.approval.level2Approved
    msg := "Level 2 approval required for sharing confidential data externally"
}

# Rate limiting rules
rate_limit_exceeded if {
    input.action == "download"
    daily_downloads := data.usage[input.user.id].daily_downloads
    daily_downloads > 100
}

# Data masking rules
mask_fields[field] if {
    input.action == "read"
    input.user.attributes.clearanceLevel < 3
    field := input.resource_attributes.piiFields[_]
}

# Audit requirements
require_audit if {
    input.resource_attributes.classification in ["CONFIDENTIAL", "SECRET"]
}

require_audit if {
    input.action in ["delete", "export", "share_external"]
}
```

## 4. Monitoring & Observability

### Prometheus Metrics

```go
// metrics.go
package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    // Request metrics
    RequestDuration = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "dcs_request_duration_seconds",
            Help: "Request duration in seconds",
            Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
        },
        []string{"service", "method", "status"},
    )
    
    RequestsTotal = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "dcs_requests_total",
            Help: "Total number of requests",
        },
        []string{"service", "method", "status"},
    )
    
    // Data processing metrics
    DatasetSize = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "dcs_dataset_size_bytes",
            Help: "Size of processed datasets in bytes",
            Buckets: prometheus.ExponentialBuckets(1024, 10, 8),
        },
        []string{"tenant", "format"},
    )
    
    ProcessingDuration = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "dcs_processing_duration_seconds",
            Help: "Processing duration in seconds",
            Buckets: prometheus.ExponentialBuckets(1, 2, 10),
        },
        []string{"operation", "tenant"},
    )
    
    ValidationErrors = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "dcs_validation_errors_total",
            Help: "Total validation errors",
        },
        []string{"error_type", "tenant"},
    )
    
    // Workflow metrics
    WorkflowDuration = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "dcs_workflow_duration_seconds",
            Help: "Workflow execution duration",
            Buckets: prometheus.ExponentialBuckets(60, 2, 10),
        },
        []string{"workflow_type", "status"},
    )
    
    ApprovalQueueDepth = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "dcs_approval_queue_depth",
            Help: "Number of pending approvals",
        },
        []string{"level", "tenant"},
    )
    
    // System metrics
    CacheHitRatio = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "dcs_cache_hit_ratio",
            Help: "Cache hit ratio",
        },
        []string{"cache_name"},
    )
    
    DatabaseConnections = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "dcs_database_connections",
            Help: "Number of database connections",
        },
        []string{"database", "status"},
    )
    
    KafkaLag = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "dcs_kafka_consumer_lag",
            Help: "Kafka consumer lag",
        },
        []string{"topic", "partition", "consumer_group"},
    )
)

// Custom metrics collector
type CustomCollector struct {
    processingQueueLength *prometheus.Desc
    activeUsers          *prometheus.Desc
}

func NewCustomCollector() *CustomCollector {
    return &CustomCollector{
        processingQueueLength: prometheus.NewDesc(
            "dcs_processing_queue_length",
            "Current processing queue length",
            []string{"priority"},
            nil,
        ),
        activeUsers: prometheus.NewDesc(
            "dcs_active_users",
            "Number of active users",
            []string{"tenant"},
            nil,
        ),
    }
}

func (c *CustomCollector) Describe(ch chan<- *prometheus.Desc) {
    ch <- c.processingQueueLength
    ch <- c.activeUsers
}

func (c *CustomCollector) Collect(ch chan<- prometheus.Metric) {
    // Collect processing queue metrics
    queueLengths := getQueueLengths()
    for priority, length := range queueLengths {
        ch <- prometheus.MustNewConstMetric(
            c.processingQueueLength,
            prometheus.GaugeValue,
            float64(length),
            priority,
        )
    }
    
    // Collect active user metrics
    activeUsers := getActiveUsersByTenant()
    for tenant, count := range activeUsers {
        ch <- prometheus.MustNewConstMetric(
            c.activeUsers,
            prometheus.GaugeValue,
            float64(count),
            tenant,
        )
    }
}
```

### Distributed Tracing

```python
# tracing.py
from opentelemetry import trace
from opentelemetry.exporter.jaeger import JaegerExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.kafka import KafkaInstrumentor
import functools

# Initialize tracing
def init_tracing(service_name: str, jaeger_endpoint: str):
    trace.set_tracer_provider(TracerProvider())
    tracer = trace.get_tracer(service_name)
    
    # Configure Jaeger exporter
    jaeger_exporter = JaegerExporter(
        agent_host_name=jaeger_endpoint,
        agent_port=6831,
    )
    
    span_processor = BatchSpanProcessor(jaeger_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)
    
    # Auto-instrument libraries
    SQLAlchemyInstrumentor().instrument()
    RedisInstrumentor().instrument()
    KafkaInstrumentor().instrument()
    
    return tracer

# Tracing decorator
def trace_operation(operation_name: str = None):
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            tracer = trace.get_tracer(__name__)
            op_name = operation_name or f"{func.__module__}.{func.__name__}"
            
            with tracer.start_as_current_span(op_name) as span:
                # Add span attributes
                span.set_attribute("function.name", func.__name__)
                span.set_attribute("function.module", func.__module__)
                
                # Extract tenant_id if available
                if 'tenant_id' in kwargs:
                    span.set_attribute("tenant.id", kwargs['tenant_id'])
                
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(trace.Status(trace.StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(
                        trace.Status(trace.StatusCode.ERROR, str(e))
                    )
                    span.record_exception(e)
                    raise
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            tracer = trace.get_tracer(__name__)
            op_name = operation_name or f"{func.__module__}.{func.__name__}"
            
            with tracer.start_as_current_span(op_name) as span:
                span.set_attribute("function.name", func.__name__)
                span.set_attribute("function.module", func.__module__)
                
                try:
                    result = func(*args, **kwargs)
                    span.set_status(trace.Status(trace.StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(
                        trace.Status(trace.StatusCode.ERROR, str(e))
                    )
                    span.record_exception(e)
                    raise
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator

# Usage example
class DataProcessingService:
    @trace_operation("process_dataset")
    async def process_dataset(self, dataset_id: str, tenant_id: str):
        span = trace.get_current_span()
        
        # Add custom attributes
        span.set_attribute("dataset.id", dataset_id)
        span.set_attribute("processing.start_time", datetime.utcnow().isoformat())
        
        # Create child spans for sub-operations
        with trace.get_tracer(__name__).start_as_current_span("load_dataset") as load_span:
            dataset = await self.load_dataset(dataset_id)
            load_span.set_attribute("dataset.size", len(dataset))
        
        with trace.get_tracer(__name__).start_as_current_span("validate_dataset") as validate_span:
            validation_result = await self.validate(dataset)
            validate_span.set_attribute("validation.passed", validation_result.passed)
            validate_span.set_attribute("validation.errors", len(validation_result.errors))
        
        with trace.get_tracer(__name__).start_as_current_span("transform_dataset") as transform_span:
            transformed = await self.transform(dataset)
            transform_span.set_attribute("transform.records_processed", len(transformed))
        
        return transformed
```

## 5. Disaster Recovery Implementation

### Backup and Recovery Scripts

```bash
#!/bin/bash
# backup.sh - Automated backup script

set -euo pipefail

# Configuration
ENVIRONMENT=${1:-production}
BACKUP_TYPE=${2:-full}  # full or incremental
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups/${ENVIRONMENT}/${TIMESTAMP}"

# Function to backup PostgreSQL
backup_postgresql() {
    echo "Starting PostgreSQL backup..."
    
    # Create backup
    PGPASSWORD=${DB_PASSWORD} pg_dump \
        -h ${DB_HOST} \
        -U ${DB_USER} \
        -d ${DB_NAME} \
        --format=custom \
        --verbose \
        --no-owner \
        --no-acl \
        --clean \
        --if-exists \
        > ${BACKUP_DIR}/postgres_${TIMESTAMP}.dump
    
    # Compress and encrypt
    gzip -9 ${BACKUP_DIR}/postgres_${TIMESTAMP}.dump
    gpg --encrypt --recipient backup@dcs.io \
        ${BACKUP_DIR}/postgres_${TIMESTAMP}.dump.gz
    
    # Upload to S3
    aws s3 cp ${BACKUP_DIR}/postgres_${TIMESTAMP}.dump.gz.gpg \
        s3://dcs-backups/${ENVIRONMENT}/postgres/ \
        --storage-class GLACIER
}

# Function to backup MongoDB
backup_mongodb() {
    echo "Starting MongoDB backup..."
    
    mongodump \
        --uri="${MONGO_URI}" \
        --out=${BACKUP_DIR}/mongo \
        --gzip \
        --oplog
    
    # Create archive
    tar czf ${BACKUP_DIR}/mongo_${TIMESTAMP}.tar.gz \
        -C ${BACKUP_DIR} mongo
    
    # Encrypt
    gpg --encrypt --recipient backup@dcs.io \
        ${BACKUP_DIR}/mongo_${TIMESTAMP}.tar.gz
    
    # Upload to S3
    aws s3 cp ${BACKUP_DIR}/mongo_${TIMESTAMP}.tar.gz.gpg \
        s3://dcs-backups/${ENVIRONMENT}/mongo/
}

# Function to backup Kafka topics
backup_kafka() {
    echo "Starting Kafka backup..."
    
    # Get list of topics
    TOPICS=$(kafka-topics.sh --list --bootstrap-server ${KAFKA_BROKERS})
    
    for topic in ${TOPICS}; do
        # Export topic data
        kafka-console-consumer.sh \
            --bootstrap-server ${KAFKA_BROKERS} \
            --topic ${topic} \
            --from-beginning \
            --max-messages 1000000 \
            --property print.timestamp=true \
            --property print.key=true \
            --property print.value=true \
            > ${BACKUP_DIR}/kafka_${topic}_${TIMESTAMP}.json
        
        # Compress
        gzip -9 ${BACKUP_DIR}/kafka_${topic}_${TIMESTAMP}.json
    done
    
    # Create archive
    tar czf ${BACKUP_DIR}/kafka_${TIMESTAMP}.tar.gz \
        ${BACKUP_DIR}/kafka_*.json.gz
    
    # Upload to S3
    aws s3 cp ${BACKUP_DIR}/kafka_${TIMESTAMP}.tar.gz \
        s3://dcs-backups/${ENVIRONMENT}/kafka/
}

# Function to backup configurations
backup_configs() {
    echo "Starting configuration backup..."
    
    # Backup Kubernetes configs
    kubectl get all --all-namespaces -o yaml > ${BACKUP_DIR}/k8s_resources.yaml
    kubectl get configmap --all-namespaces -o yaml > ${BACKUP_DIR}/k8s_configmaps.yaml
    kubectl get secret --all-namespaces -o yaml > ${BACKUP_DIR}/k8s_secrets.yaml
    
    # Backup Terraform state
    terraform show -json > ${BACKUP_DIR}/terraform_state.json
    
    # Create archive
    tar czf ${BACKUP_DIR}/configs_${TIMESTAMP}.tar.gz \
        ${BACKUP_DIR}/*.yaml \
        ${BACKUP_DIR}/*.json
    
    # Upload to S3
    aws s3 cp ${BACKUP_DIR}/configs_${TIMESTAMP}.tar.gz \
        s3://dcs-backups/${ENVIRONMENT}/configs/
}

# Main execution
main() {
    echo "Starting backup for environment: ${ENVIRONMENT}"
    echo "Backup type: ${BACKUP_TYPE}"
    echo "Timestamp: ${TIMESTAMP}"
    
    # Create backup directory
    mkdir -p ${BACKUP_DIR}
    
    # Run backups in parallel
    backup_postgresql &
    PG_PID=$!
    
    backup_mongodb &
    MONGO_PID=$!
    
    backup_kafka &
    KAFKA_PID=$!
    
    backup_configs &
    CONFIG_PID=$!
    
    # Wait for all backups to complete
    wait ${PG_PID} ${MONGO_PID} ${KAFKA_PID} ${CONFIG_PID}
    
    # Verify backups
    echo "Verifying backups..."
    aws s3 ls s3://dcs-backups/${ENVIRONMENT}/ --recursive \
        | grep ${TIMESTAMP}
    
    # Send notification
    send_notification "Backup completed successfully for ${ENVIRONMENT}"
    
    # Cleanup local files
    rm -rf ${BACKUP_DIR}
    
    echo "Backup completed successfully"
}

# Run main function
main
```

This comprehensive technical implementation guide provides detailed code examples, configurations, and patterns for building the Data Collaboration Studio. The implementation covers all major architectural components with production-ready code that can be adapted to specific requirements.