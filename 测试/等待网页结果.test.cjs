const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const script = path.join(__dirname, '../网页会话池/等待网页结果.js');
const load = () => {
  assert.ok(fs.existsSync(script), '尚未提供可复用的等待程序');
  return new Function('return (' + fs.readFileSync(script, 'utf8') + ')')();
};
const options = {jobId:'job', threadId:'thread', dispatchTurnId:'dispatch',
  baselineMessageId:'old', intervalMs:1000, maxWaitMs:3000};
function page(state, id='old', turn='dispatch', thread='thread') {
  return {content:[{type:'text', text:JSON.stringify({thread:{id:thread,status:{type:state}},
    turns:[{id:turn,status:'completed',items:[{type:'agentMessage',id,text:'答复'}]}]})}]};
}
// 只替换外部会话服务及时间，真正的判断、循环、结果构造使用生产代码。
function harness(pages) {
  let time=0, reads=0, refreshed=false;
  return {
    runtime:{now:()=>time, sleep:async ms=>{time+=ms;}},
    tools:{mcp__codex_app__list_threads:async()=>{refreshed=true;return {content:[]};},
      mcp__codex_app__read_thread:async args=>{
        assert.equal(args.threadId,'thread'); assert.ok(refreshed); refreshed=false;
        return pages[Math.min(reads++,pages.length-1)];
      }},
  };
}
test('活动中的新答复不提前交付，空闲后才返回候选', async()=>{
  const h=harness([page('active','new'),page('idle','new')]);
  const r=await load()(h.tools,options,h.runtime);
  assert.equal(r.status,'candidate');assert.equal(r.messageId,'new');assert.equal(r.polls,2);
  assert.equal(r.accepted,false);
});
test('旧答复和别的派发轮次不能当成本次完成', async()=>{
  for (const p of [page('idle'),page('idle','new','other')]) {
    const h=harness([p]);const r=await load()(h.tools,options,h.runtime);
    assert.equal(r.status,'timeout');assert.equal(r.messageId,undefined);
  }
});
test('会话错配和不可识别状态立即报告协议错误', async()=>{
  for (const p of [page('idle','new','dispatch','wrong'),page('mystery','new')]) {
    const h=harness([p]);assert.equal((await load()(h.tools,options,h.runtime)).status,'protocol_error');
  }
});
test('接口报错不伪造空闲、不无限重试', async()=>{
  const h=harness([{isError:true,content:[{type:'text',text:'拒绝访问'}]}]);
  const r=await load()(h.tools,options,h.runtime);
  assert.equal(r.status,'read_error');assert.equal(r.polls,1);
});
test('停止状态返回需关注而非验收成功', async()=>{
  const h=harness([page('failed','new')]);
  assert.equal((await load()(h.tools,options,h.runtime)).status,'attention');
});
test('缺少基线、过短间隔或无限等待参数被拒绝', async()=>{
  for (const change of [{dispatchTurnId:''},{intervalMs:0},{maxWaitMs:Infinity}]) {
    const h=harness([page('idle','new')]);
    await assert.rejects(()=>load()(h.tools,{...options,...change},h.runtime));
  }
});
