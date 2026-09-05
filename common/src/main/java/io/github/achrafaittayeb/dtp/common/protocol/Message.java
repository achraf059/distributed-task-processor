package io.github.achrafaittayeb.dtp.common.protocol;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * All wire messages exchanged in the system. Each frame carries exactly one
 * message serialized as JSON with a discriminating {@code type} field, e.g.
 *
 * <pre>{"type":"HEARTBEAT","workerId":"worker-1"}</pre>
 *
 * The hierarchy is sealed so the compiler enforces exhaustive handling in
 * {@code switch} dispatch, and Jackson rejects unknown {@code type} values,
 * which surfaces as a {@link io.github.achrafaittayeb.dtp.common.net.ProtocolException}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        // Worker -> Coordinator
        @JsonSubTypes.Type(value = WorkerRegister.class, name = "WORKER_REGISTER"),
        @JsonSubTypes.Type(value = Heartbeat.class, name = "HEARTBEAT"),
        @JsonSubTypes.Type(value = TaskResult.class, name = "TASK_RESULT"),
        // Coordinator -> Worker
        @JsonSubTypes.Type(value = WorkerRegistered.class, name = "WORKER_REGISTERED"),
        @JsonSubTypes.Type(value = TaskAssign.class, name = "TASK_ASSIGN"),
        @JsonSubTypes.Type(value = TaskCancel.class, name = "TASK_CANCEL"),
        // Client -> Coordinator
        @JsonSubTypes.Type(value = SubmitJob.class, name = "SUBMIT_JOB"),
        @JsonSubTypes.Type(value = GetJobStatus.class, name = "GET_JOB_STATUS"),
        @JsonSubTypes.Type(value = ListJobs.class, name = "LIST_JOBS"),
        @JsonSubTypes.Type(value = ListWorkers.class, name = "LIST_WORKERS"),
        @JsonSubTypes.Type(value = CancelJob.class, name = "CANCEL_JOB"),
        // Coordinator -> Client
        @JsonSubTypes.Type(value = JobSubmitted.class, name = "JOB_SUBMITTED"),
        @JsonSubTypes.Type(value = JobStatusReply.class, name = "JOB_STATUS"),
        @JsonSubTypes.Type(value = JobListReply.class, name = "JOB_LIST"),
        @JsonSubTypes.Type(value = WorkerListReply.class, name = "WORKER_LIST"),
        @JsonSubTypes.Type(value = JobCancelReply.class, name = "JOB_CANCEL"),
        // Either direction
        @JsonSubTypes.Type(value = ErrorReply.class, name = "ERROR"),
})
public sealed interface Message
        permits WorkerRegister, Heartbeat, TaskResult,
        WorkerRegistered, TaskAssign, TaskCancel,
        SubmitJob, GetJobStatus, ListJobs, ListWorkers, CancelJob,
        JobSubmitted, JobStatusReply, JobListReply, WorkerListReply, JobCancelReply,
        ErrorReply {
}
