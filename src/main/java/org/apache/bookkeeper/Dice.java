package org.apache.bookkeeper;


import com.fishjam.util.debug.DebugUtil;
import com.google.common.primitives.Ints;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;


public class Dice extends LeaderSelectorListenerAdapter implements Closeable, LeaderLatchListener {

  final static String ZOOKEEPER_SERVER = "127.0.0.1:2181";
  final static String ELECTION_PATH = "/dice-elect";
  final static byte[] DICE_PASSWD = "dice".getBytes();
  final static String DICE_LOG = "/dice-log";

  //Random r = new Random();
  int globalNextInt = 0;
  CuratorFramework curator;
  //LeaderSelector leaderSelector;
  LeaderLatch leaderLatch;
  BookKeeper bookkeeper;

  volatile boolean leader = false;

  Dice() throws Exception {
    DebugUtil.log("Enter Dice()");
    curator = CuratorFrameworkFactory.newClient(ZOOKEEPER_SERVER,
        2000, 10000, new ExponentialBackoffRetry(1000, 3));
    curator.start();
    curator.blockUntilConnected();

    try {
      curator.create().forPath(ELECTION_PATH);
    }catch(KeeperException.NodeExistsException e){
      System.out.println("ELECTION_PATH exist, just ignore");
    }


    DebugUtil.log("begin LeaderSelector");
    //leaderSelector = new LeaderSelector(curator, ELECTION_PATH, this);
    //leaderSelector.autoRequeue();
    //leaderSelector.start();

    leaderLatch = new LeaderLatch(curator, ELECTION_PATH);
    leaderLatch.addListener(this);
    leaderLatch.start();

    DebugUtil.log("before getLeader");
    //Participant leader = leaderSelector.getLeader();
    Participant leader = leaderLatch.getLeader();
    DebugUtil.log("after select start, leader is:" + this.leader);

    ClientConfiguration conf = new ClientConfiguration()
        .setZkServers(ZOOKEEPER_SERVER).setZkTimeout(30000);
    bookkeeper = new BookKeeper(conf);
  }

  @Override
  public void takeLeadership(CuratorFramework client)
      throws Exception {
    synchronized (this) {
      //DebugUtil.log(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Becoming leader(%s)", leaderSelector.getLeader());
      //Thread.sleep(3000); //测试选举会话费时间的情况
      leader = true;
      try {
        while (true) {
          this.wait();
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        leader = false;
        ie.printStackTrace();
        //DebugUtil.log(ie.toString());
      }
      DebugUtil.log("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<Losing leader");
    }
  }

  @Override
  public void isLeader() {
    leader = true;
    try {
      DebugUtil.log(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>> isLeader: %s", leaderLatch.getLeader());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void notLeader() {
    leader = false;
    try {
      DebugUtil.log("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< notLeader: %s", leaderLatch.getLeader());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  @Override
  public void close() {
    DebugUtil.log("Enter close, this is never execute");
    //leaderSelector.close();
    try {
      leaderLatch.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    curator.close();
  }

  EntryId lead(EntryId skipPast) throws Exception {
    DebugUtil.log("Enter >>>> Dice.lead, skipPast = [" + skipPast + "]");

    EntryId lastDisplayedEntry = skipPast;
    //Stat stat = new Stat();
    List<Long> ledgers;
    boolean mustCreate = false;
    try {
      byte[] ledgerListBytes = curator.getData()
          //.storingStatIn(stat)
          .forPath(DICE_LOG);
      ledgers = listFromBytes(ledgerListBytes);
    } catch (KeeperException.NoNodeException nne) {
      //初次执行时,没有这个节点
      ledgers = new ArrayList<Long>();
      mustCreate = true;
      //nne.printStackTrace();
    }

    DebugUtil.log("ledgers=%s", Arrays.toString(ledgers.toArray()));

    List<Long> toRead = ledgers;
    if (skipPast.getLedgerId() != -1) {
      toRead = ledgers.subList(ledgers.indexOf(skipPast.getLedgerId()),
          ledgers.size());
    }

    long nextEntry = skipPast.getEntryId() + 1;
    for (Long previous : toRead) {
      LedgerHandle lh;
      try {
        lh = bookkeeper.openLedger(previous,
            BookKeeper.DigestType.MAC, DICE_PASSWD);
      } catch (BKException.BKLedgerRecoveryException e) {
        //DebugUtil.log(e.toString());
        e.printStackTrace();
        return lastDisplayedEntry;
      }

      if (nextEntry > lh.getLastAddConfirmed()) {
        DebugUtil.log("Warn: leader, nextEntry(%d) Bigger than lh.getLastAddConfirmed(%d), skipPast=%s, ledgerId=%d",
            nextEntry, lh.getLastAddConfirmed(), skipPast, lh.getId());
        nextEntry = 0;
        continue;
      }
      Enumeration<LedgerEntry> entries
          = lh.readEntries(nextEntry, lh.getLastAddConfirmed());

      while (entries.hasMoreElements()) {
        LedgerEntry e = entries.nextElement();
        byte[] entryData = e.getEntry();
        globalNextInt = Ints.fromByteArray(entryData);
        DebugUtil.log("Value = " + globalNextInt
            + ", epoch = " + lh.getId()
            + ", catchup");
        lastDisplayedEntry = new EntryId(lh.getId(), e.getEntryId());
      }
    }

    LedgerHandle lh = bookkeeper.createLedger(3, 3, 2,
        BookKeeper.DigestType.MAC, DICE_PASSWD);
    ledgers.add(lh.getId());
    byte[] ledgerListBytes = listToBytes(ledgers);
    if (mustCreate) {
      try {
        DebugUtil.log("create node:log=%s, next=%d", Arrays.toString(ledgers.toArray()), globalNextInt);
        curator.create().forPath(DICE_LOG, ledgerListBytes);
      } catch (KeeperException.NodeExistsException nne) {
        nne.printStackTrace();
        //DebugUtil.log(nne.toString());
        return lastDisplayedEntry;
      }
    } else {
      try {
        DebugUtil.log("set node data:log=%s, next=%d", Arrays.toString(ledgers.toArray()), globalNextInt);
        curator.setData()
            //.withVersion(stat.getVersion())
            .forPath(DICE_LOG, ledgerListBytes);
      } catch (KeeperException.BadVersionException bve) {
        bve.printStackTrace();
        //DebugUtil.log(bve.toString());
        return lastDisplayedEntry;
      }
    }

    try {
      while (leader) {
        //int nextInt = r.nextInt(6) + 1;
        long entryId = lh.addEntry(Ints.toByteArray(globalNextInt));
        DebugUtil.log("Value = " + globalNextInt
            + ", epoch = " + lh.getId()
            + ", leading");
        globalNextInt++;

        lastDisplayedEntry = new EntryId(lh.getId(), entryId);
        Thread.sleep(5000);
      }
      DebugUtil.log(">>>>>>>> lost leader, now will close ledger,id=%d, lastAddCfm=%d",
          lh.getId(), lh.getLastAddConfirmed());
      lh.close();
    } catch (BKException e) {
      //Ctrl+Z 放后台再重新放到前台, addEntry 时可能发生 BKException$BKLedgerFencedException 异常?

      // let it fall through to the return
      e.printStackTrace();
      //DebugUtil.log(e.toString());
    }
    return lastDisplayedEntry;
  }

  EntryId follow(EntryId skipPast) throws Exception {
    DebugUtil.log("Enter follow................");
    List<Long> ledgers = null;
    while (ledgers == null) {
      //等待leader创建meta信息的节点 -- 在几个节点同时启动时出现
      try {
        byte[] ledgerListBytes = curator.getData()
            .forPath(DICE_LOG);
        ledgers = listFromBytes(ledgerListBytes);
        if (skipPast.getLedgerId() != -1) {
          ledgers = ledgers.subList(ledgers.indexOf(skipPast.getLedgerId()),
              ledgers.size());
        }
      } catch (KeeperException.NoNodeException nne) {
        Thread.sleep(1000);
        nne.printStackTrace();
        //DebugUtil.log(nne.toString());
      }
    }
    DebugUtil.log("follow: ledgers:=%s, skipPast=%s", Arrays.toString(ledgers.toArray()), skipPast);

    EntryId lastReadEntry = skipPast;
    while (!leader) {
      for (long previous : ledgers) {
        boolean isClosed = false;
        long nextEntry = 0;
        String strLastDiffInfo = "";
        while (!isClosed && !leader) {
          //轮询机制 -- 只要 ledger 没有关闭,就依此获取其最新值
          if (lastReadEntry.getLedgerId() == previous) {
            nextEntry = lastReadEntry.getEntryId() + 1;
          }
          isClosed = bookkeeper.isClosed(previous);
          LedgerHandle lh = bookkeeper.openLedgerNoRecovery(previous, //TODO:尝试改为 openLedger 后查看效果
              BookKeeper.DigestType.MAC, DICE_PASSWD);

          String strInfo = String.format("follow: LedgerId=%d, previous=%d, isClosed=%d, tryLastConfirm=%d, confirmed=%d"
                  + ",addpushed=%d,length=%d, nextEntry=%d, lastReadEntry=%s",
              lh.getId(), previous, isClosed? 1 : 0,
              lh.tryReadLastConfirmed(), lh.getLastAddConfirmed(),
              lh.getLastAddPushed(), lh.getLength(), nextEntry, lastReadEntry);

          if (strLastDiffInfo != strInfo) {
            //避免打印重复数据太多 -- 当前5秒写一次,1秒尝试读取一次
            DebugUtil.log(strInfo);
            //strLastDiffInfo = strInfo;
          }

          if (nextEntry <= lh.getLastAddConfirmed()) {
            //表明写入了新的值
            Enumeration<LedgerEntry> entries
                = lh.readEntries(nextEntry,
                lh.getLastAddConfirmed());
            while (entries.hasMoreElements()) {
              LedgerEntry e = entries.nextElement();
              byte[] entryData = e.getEntry();
              DebugUtil.log("Read new value = " + Ints.fromByteArray(entryData)
                  + ", epoch = " + lh.getId()
                  + ", following");
              lastReadEntry = new EntryId(previous, e.getEntryId());
            }
          }
          if (isClosed) {
            break;
          }
          Thread.sleep(1000);
        }
      }
      byte[] ledgerListBytes = curator.getData()
          .forPath(DICE_LOG);
      ledgers = listFromBytes(ledgerListBytes);
      DebugUtil.log("get new ledgers:%s, lastReadEntry=%s", Arrays.toString(ledgers.toArray()), lastReadEntry);

      ledgers = ledgers.subList(ledgers.indexOf(lastReadEntry.getLedgerId()) + 1, ledgers.size());
    }
    return lastReadEntry;
  }

  void playDice() throws Exception {
    EntryId lastDisplayedEntry = new EntryId(-1, -1);
    while (true) {
      if (leader) {
        lastDisplayedEntry = lead(lastDisplayedEntry);
      } else {
        lastDisplayedEntry = follow(lastDisplayedEntry);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Dice d = new Dice();
    try {
      d.playDice();
    } finally {
      d.close();
    }
  }

  static class EntryId {
    final long ledgerId;
    final long entryId;

    EntryId(long ledgerId, long entryId) {
      this.ledgerId = ledgerId;
      this.entryId = entryId;
    }

    long getLedgerId() {
      return ledgerId;
    }

    long getEntryId() {
      return entryId;
    }

    @Override
    public String toString() {
      return "EntryId{" +
          "ledgerId=" + ledgerId +
          ", entryId=" + entryId +
          '}';
    }
  }

  static byte[] listToBytes(List<Long> ledgerIds) {
    ByteBuffer bb = ByteBuffer.allocate((Long.SIZE * ledgerIds.size()) / 8);
    for (Long l : ledgerIds) {
      bb.putLong(l);
    }
    return bb.array();
  }

  static List<Long> listFromBytes(byte[] bytes) {
    List<Long> ledgerIds = new ArrayList<Long>();
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    while (bb.remaining() > 0) {
      ledgerIds.add(bb.getLong());
    }
    return ledgerIds;
  }
}
