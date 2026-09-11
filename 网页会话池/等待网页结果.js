async function waitWebResult(tools, options, runtime) {
  // 由 Codex 工具执行环境加载；不自行获取凭据、不发送消息、不写会话池。
  const {jobId, threadId, dispatchTurnId, baselineMessageId = null,
    intervalMs = 15000, maxWaitMs = 900000} = options;
  for (const value of [jobId, threadId, dispatchTurnId]) {
    if (typeof value !== 'string' || !value.trim()) throw new Error('缺少任务或派发轮次标识');
  }
  if (baselineMessageId !== null && typeof baselineMessageId !== 'string') {
    throw new Error('基线答复标识必须为字符串或 null');
  }
  if (!Number.isFinite(intervalMs) || intervalMs < 1000 || intervalMs > 60000 ||
      !Number.isFinite(maxWaitMs) || maxWaitMs < 1 || maxWaitMs > 3600000) {
    throw new Error('检查间隔须为1至60秒，单次等待须在1小时内');
  }
  if (!runtime || typeof runtime.now !== 'function' || typeof runtime.sleep !== 'function') {
    throw new Error('必须由宿主提供时钟和异步等待');
  }
  const started = runtime.now();
  let polls = 0, lastState = null;
  const result = (status, extra = {}) => ({status, jobId, threadId, dispatchTurnId,
    polls, elapsedMs: runtime.now() - started, lastState, accepted: false, ...extra});
  while (runtime.now() - started < maxWaitMs) {
    let response;
    polls++;
    try {
      const refreshed = await tools.mcp__codex_app__list_threads({limit: 1});
      if (refreshed.isError) return result('read_error', {stage: 'refresh'});
      response = await tools.mcp__codex_app__read_thread({threadId, turnLimit: 5,
        maxOutputCharsPerItem: 2000});
      if (response.isError) return result('read_error', {stage: 'read'});
    } catch (error) {
      // 不打印可能包含凭据或业务正文的原始异常；不自动重试发送。
      return result('read_error', {errorType: error?.name || 'Error'});
    }
    let data;
    try {
      const block = response.content?.find(item => item.type === 'text');
      data = JSON.parse(block.text);
      if (data.thread?.id !== threadId || !Array.isArray(data.turns)) throw new Error();
      lastState = data.thread.status?.type;
      if (!['active', 'idle', 'failed', 'interrupted', 'needs_attention'].includes(lastState)) {
        throw new Error();
      }
    } catch {
      return result('protocol_error');
    }
    const turn = data.turns.find(item => item.id === dispatchTurnId);
    if (['failed', 'interrupted', 'needs_attention'].includes(lastState) ||
        turn?.error || ['failed', 'interrupted'].includes(turn?.status)) {
      return result('attention');
    }
    // 用户轮次的 completed 不表示生成完成；必须同时检查顶层空闲和新助手答复。
    const answer = turn?.items?.filter(item => item.type === 'agentMessage' &&
      typeof item.id === 'string' && item.id !== baselineMessageId &&
      typeof item.text === 'string' && item.text.trim()).at(-1);
    if (lastState === 'idle' && answer) {
      return result('candidate', {messageId: answer.id,
        preview: answer.text, truncated: Boolean(answer.truncated)});
    }
    const remaining = maxWaitMs - (runtime.now() - started);
    if (remaining > 0) await runtime.sleep(Math.min(intervalMs, remaining));
  }
  return result('timeout');
}
