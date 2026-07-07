# Atomic DAG Orchestration - Child Enqueue Fix

## 🎯 Problem

When a parent task completes, children need to be enqueued to Redis. But if the parent is marked `COMPLETED` in the database **before** enqueueing children to Redis, a failure in between leaves the system in an inconsistent state.

### **Visual Comparison:**

```
❌ BEFORE (Non-Atomic):
┌─────────────────┐
│ Process Task A  │ ✅
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Mark A COMPLETE │ ✅ (DB write)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Find Children   │ ✅
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Enqueue Child B │ ❌ REDIS DOWN!
└─────────────────┘

Result: A is COMPLETED, but B never enqueued → B stuck forever!
```

```
✅ AFTER (Atomic):
┌─────────────────┐
│ Process Task A  │ ✅
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Find Children   │ ✅
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Enqueue Child B │ ❌ REDIS DOWN!
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Mark A COMPLETE │ ⏸️  SKIPPED (abort)
└─────────────────┘

Result: A stays RUNNING → Janitor retries → Eventually succeeds
```

### **The Issue:**

```
❌ BEFORE (Broken Order):
1. CompleteJob()           → Parent marked COMPLETED in DB
2. CheckAndEnqueueChildren() → Find ready children
3. EnqueueTask()           → Push to Redis ⚠️ FAILS HERE
4. AcknowledgeTask()       → Clean up parent

Result: Parent is COMPLETED but children never enqueued → Children stuck forever!
```

---

## ✅ Solution: Reverse the Order

Since **PostgreSQL and Redis can't share a transaction**, we reverse the operation order so failures are recoverable:

```
✅ AFTER (Fixed Order):
1. CheckAndEnqueueChildren() → Find ready children
2. EnqueueTask()           → Push to Redis FIRST ⚠️ If fails here...
3. CompleteJob()           → Mark parent COMPLETED AFTER
4. AcknowledgeTask()       → Clean up parent

Result: If EnqueueTask fails, parent stays RUNNING → Janitor will retry the whole flow
```

---

## 🔧 How It Works

### **Success Path:**
```go
// 1. Check for children
children := CheckAndEnqueueChildren(taskId)  // ["child1", "child2"]

// 2. Enqueue all children to Redis
for child in children:
    EnqueueTask(child)  // ✅ All succeed

// 3. Mark parent complete
CompleteJob(taskId)  // ✅ Parent now COMPLETED

// 4. Clean up
AcknowledgeTask(taskId)  // ✅ Parent removed from inflight
```

### **Failure Path (Redis Down):**
```go
// 1. Check for children
children := CheckAndEnqueueChildren(taskId)  // ["child1", "child2"]

// 2. Try to enqueue
EnqueueTask("child1")  // ✅ Success
EnqueueTask("child2")  // ❌ REDIS DOWN!

// 3. Abort! Don't mark parent complete
continue  // Leave parent in RUNNING state

// Later: Janitor sweeps parent back to queue
// Worker retries entire flow
// This time Redis is up → children enqueued → parent completed
```

---

## 📊 Key Points

### **Why This Works:**

1. **Idempotent Enqueue**: `EnqueueTask()` is safe to call multiple times
2. **Parent Stays Claimable**: If children fail to enqueue, parent remains `RUNNING`
3. **Janitor Recovery**: Parent stays in Redis `:inflight`, janitor will retry after timeout
4. **All-or-Nothing**: Either ALL children enqueue + parent completes, or NOTHING changes

### **Trade-off:**

- **Before**: Fast but inconsistent (parent marked done even if children fail)
- **After**: Slightly slower but consistent (parent only marked done if children succeed)

---

## 🧪 Test Scenarios

### **Test 1: Normal Success**
```go
// Parent completes
taskA.process()  // Success

// Children enqueued
assert redis.contains("taskB")  // ✅
assert redis.contains("taskC")  // ✅

// Parent marked complete
assert taskA.status == COMPLETED  // ✅
```

### **Test 2: Redis Failure**
```go
// Parent completes processing
taskA.process()  // Success

// Redis goes down
redis.stop()

// Try to enqueue children
worker.completeTask(taskA)  // Tries to enqueue children → FAILS

// Verify parent NOT marked complete
assert taskA.status == RUNNING  // ✅ Still RUNNING
assert redis_inflight.contains(taskA)  // ✅ Still in inflight

// Janitor recovers
time.sleep(holdTTL + 1)
janitor.sweep()
assert redis_queue.contains(taskA)  // ✅ Back in queue

// Redis comes back up
redis.start()

// Worker retries
worker.consume()  // Gets taskA again
worker.completeTask(taskA)  // Now succeeds
assert taskA.status == COMPLETED  // ✅
assert redis.contains("taskB")  // ✅
```

### **Test 3: Partial Enqueue Failure**
```go
// Parent has 3 children
taskA.children = [taskB, taskC, taskD]

// First 2 succeed, 3rd fails
EnqueueTask(taskB)  // ✅
EnqueueTask(taskC)  // ✅
EnqueueTask(taskD)  // ❌ Network error

// Verify parent NOT marked complete
assert taskA.status == RUNNING  // ✅
assert redis.contains(taskB)  // ⚠️ taskB in Redis twice after retry
assert redis.contains(taskC)  // ⚠️ taskC in Redis twice after retry

// Worker retries entire flow
// CheckAndEnqueueChildren returns same children
// Re-enqueues taskB, taskC (duplicates in Redis)
// Enqueues taskD (now succeeds)
// Marks parent complete

// Idempotency check handles duplicates:
// First worker processes taskB → marks COMPLETED
// Second worker gets duplicate taskB → sees COMPLETED → drops it
```

---

## 🚨 Edge Cases Handled

### **1. Duplicate Children in Redis**
**Problem**: Retry could put same child in Redis multiple times

**Solution**: Idempotency check in `TryStartJob()`
```go
status := TryStartJob(taskB)
if status == COMPLETED:
    // Already processed by another worker
    AcknowledgeTask(taskB)  // Drop duplicate
    continue
```

### **2. Database Timeout During CompleteJob**
**Problem**: Children enqueued but DB times out marking parent complete

**Solution**: Parent stays in inflight, janitor retries
- Children might get processed before parent is marked complete
- When parent is finally marked complete, `CheckAndEnqueueChildren()` returns empty (children already RUNNING/COMPLETED)
- No duplicates created

### **3. Worker Crash Between Steps**
**Problem**: Worker crashes after enqueuing children but before CompleteJob

**Solution**: Same as Redis failure - parent stays RUNNING, janitor retries

---

## 📈 Performance Impact

### **Before:**
- CompleteJob: 1 DB write (parent status)
- CheckAndEnqueueChildren: 1 DB read
- EnqueueTask: N Redis writes (N = children count)
- **Total**: 1 write + 1 read + N Redis ops

### **After:**
- CheckAndEnqueueChildren: 1 DB read
- EnqueueTask: N Redis writes
- CompleteJob: 1 DB write
- **Total**: Same operations, just reordered

**No performance penalty** - same number of operations, just safer ordering!

---

## 🔍 Monitoring

### **Metrics to Watch:**

```prometheus
# Children successfully enqueued
queue_children_enqueued_total{queue="queue:video_processing"}

# Children enqueue failures (indicates Redis issues)
queue_child_enqueue_failures_total{queue="queue:video_processing"}

# Tasks swept by janitor (includes retries due to enqueue failures)
queue_tasks_swept_total{queue="queue:video_processing"}
```

### **Alerting:**

```yaml
# Alert if child enqueue failure rate > 5%
- alert: HighChildEnqueueFailureRate
  expr: |
    rate(queue_child_enqueue_failures_total[5m]) 
    / 
    rate(queue_children_enqueued_total[5m]) > 0.05
  annotations:
    summary: "High failure rate enqueueing child tasks"
    description: "Redis might be experiencing issues"
```

---

## ✅ Summary

| Aspect | Before | After |
|--------|--------|-------|
| **Order** | Complete → Enqueue | Enqueue → Complete |
| **Failure Handling** | ❌ Lost children | ✅ Retry via janitor |
| **Consistency** | ❌ Parent done, children missing | ✅ Atomic: all or nothing |
| **Recovery** | ❌ Manual intervention | ✅ Automatic retry |
| **Performance** | Same | Same |

**The fix ensures DAG orchestration is eventually consistent and recoverable from any failure point.**
