# Graceful Shutdown Implementation

## 🎯 How It Works

### **Shutdown Sequence:**

```
1. User sends SIGTERM/SIGINT (Ctrl+C)
   ↓
2. Context cancelled (ctx.Done())
   ↓
3. All goroutines check context and stop accepting new work
   ↓
4. Workers finish current in-flight tasks
   ↓
5. Main waits for WaitGroup with timeout (30s default)
   ↓
6. Either: All goroutines exit OR timeout reached
   ↓
7. Close database connections
   ↓
8. Exit
```

---

## ⏱️ **Timeout Behavior**

### **Configuration:**
```env
SHUTDOWN_TIMEOUT=30s
```

### **Scenario 1: Clean Shutdown**
```
10:00:00 - SIGTERM received
10:00:00 - Worker processing task (2s remaining)
10:00:02 - Worker finishes task
10:00:02 - Worker checks context → cancelled
10:00:02 - Worker exits
10:00:02 - All goroutines exited gracefully ✅
```

### **Scenario 2: Timeout Reached**
```
10:00:00 - SIGTERM received
10:00:00 - Worker stuck on long task
10:00:30 - Shutdown timeout reached!
10:00:30 - Force exit ⚠️
10:00:30 - Task left in Redis :inflight
10:00:50 - Janitor recovers task (after HoldTTL)
```

---

## 🛡️ **Safety Guarantees**

### **Protected:**
✅ Tasks in queue - Remain in Redis
✅ Completed tasks - Already in DB
✅ Tasks finishing within timeout - Complete normally

### **Not Protected:**
⚠️ Tasks mid-processing at timeout - Recovered by janitor

---

## 🔧 **Configuration Tuning**

```env
# Short tasks (< 5s)
SHUTDOWN_TIMEOUT=10s

# Long tasks (30-60s)
SHUTDOWN_TIMEOUT=90s

# Default (current)
SHUTDOWN_TIMEOUT=30s
```

---

## 📝 **Logs**

### **Clean:**
```json
{"msg": "shutdown signal received"}
{"msg": "all threads exited gracefully"}
{"msg": "shutdown complete"}
```

### **Timeout:**
```json
{"msg": "shutdown signal received"}
{"level": "warn", "msg": "shutdown timeout reached", "timeout": "30s"}
{"msg": "shutdown complete"}
```

---

## 🎯 **Summary**

Workers finish current tasks (up to 30s), then exit. If timeout reached, force exit and janitor recovers abandoned tasks. Zero data loss due to idempotency!
