package cloud.google.taskqueue;

public class QueueItem {

	private String kind = "taskqueues#task";
	private String id = "";
	private String queueName = "";
	private String payloadBase64 = "";
	private long enqueueTimestamp = 0L;
	private long leaseTimestamp = 0L;
	private int retry_count = 0;
	private String tag = "";

	public String getKind() {
		return kind;
	}

	public void setKind(String kind) {
		this.kind = kind;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public String getPayloadBase64() {
		return payloadBase64;
	}

	public void setPayloadBase64(String payloadBase64) {
		this.payloadBase64 = payloadBase64;
	}

	public long getEnqueueTimestamp() {
		return enqueueTimestamp;
	}

	public void setEnqueueTimestamp(long enqueueTimestamp) {
		this.enqueueTimestamp = enqueueTimestamp;
	}

	public long getLeaseTimestamp() {
		return leaseTimestamp;
	}

	public void setLeaseTimestamp(long leaseTimestamp) {
		this.leaseTimestamp = leaseTimestamp;
	}

	public int getRetry_count() {
		return retry_count;
	}

	public void setRetry_count(int retry_count) {
		this.retry_count = retry_count;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

}
