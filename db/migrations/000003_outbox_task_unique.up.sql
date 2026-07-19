-- A task is published at most once per normal lifecycle. The relay itself is
-- still at-least-once: a crash after Redis LPUSH but before marking delivered
-- can produce a duplicate, which the job state machine must tolerate.
CREATE UNIQUE INDEX outbox_task_id_unique ON outbox(task_id);
