/* Copyright (c) 2014, 2018, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "rpl_slave_commit_order_manager.h"
#include "my_debug.h"

#include "rpl_rli_pdb.h"     // Slave_worker
#include "debug_sync.h"      // debug_sync_set_action

Commit_order_manager::Commit_order_manager(uint32 worker_numbers)
  : m_rollback_trx(false), m_workers(worker_numbers), queue_head(QUEUE_EOF),
    queue_tail(QUEUE_EOF)
{
  MY_D("Initializing COM");
  mysql_mutex_init(key_commit_order_manager_mutex, &m_mutex, NULL);
  for (uint32 i= 0; i < worker_numbers; i++)
  {
    mysql_cond_init(key_commit_order_manager_cond, &m_workers[i].cond);
    m_workers[i].status= OCS_FINISH;
  }
}

Commit_order_manager::~Commit_order_manager()
{
  mysql_mutex_destroy(&m_mutex);

  MY_D("Destroying COM");
  for (uint32 i= 0; i < m_workers.size(); i++)
  {
    mysql_cond_destroy(&m_workers[i].cond);
  }
}

void Commit_order_manager::register_trx(Slave_worker *worker)
{
  DBUG_ENTER("Commit_order_manager::register_trx");

  mysql_mutex_lock(&m_mutex);

  MY_D("Worker "<<worker->id +1 << " registering trx, pushing to queue");
  m_workers[worker->id].status= OCS_WAIT;
  queue_push(worker->id);

  mysql_mutex_unlock(&m_mutex);
  DBUG_VOID_RETURN;
}

/**
  Waits until it becomes the queue head.

  @retval false All previous threads succeeded so this thread can go
  ahead and commit.
*/
bool Commit_order_manager::wait_for_its_turn(Slave_worker *worker,
                                                  bool all)
{
  DBUG_ENTER("Commit_order_manager::wait_for_its_turn");

  /*
    When prior transaction fail, current trx should stop and wait for signal
    to rollback itself
  */
  MY_D("Worker "<<worker->id + 1 << " entered wait_for_its_turn");
  if ((all || ending_single_stmt_trans(worker->info_thd, all) || m_rollback_trx) &&
      m_workers[worker->id].status == OCS_WAIT)
  {
    PSI_stage_info old_stage;
    mysql_cond_t *cond= &m_workers[worker->id].cond;
    THD *thd= worker->info_thd;

    DBUG_PRINT("info", ("Worker %lu is waiting for commit signal", worker->id));

    DBUG_EXECUTE_IF("delay_slave_worker_0", {
      if (worker->id == 1)
      {
        static const char act[]= "now SIGNAL signal.w1.wait_for_its_turn";
        DBUG_ASSERT(!debug_sync_set_action(thd, STRING_WITH_LEN(act)));
      }
    });
    DBUG_EXECUTE_IF("halt_slave_worker_3", {
      if (worker->id == 2)
      {
        MY_D("Worker "<< worker->id +1 <<" entering DEBUG_SYNC");
        static const char act[]= "now SIGNAL signal.w3.wait_for_its_turn WAIT_FOR go_ahead_w3";
        DBUG_ASSERT(!debug_sync_set_action(thd, STRING_WITH_LEN(act)));
        MY_D("Worker "<< worker->id +1 <<" exiting DEBUG_SYNC");
        MY_D("Worker "<< worker->id +1 <<" Worker status:" << worker->running_status << " Worker thd killed:"<< thd->killed <<" m_rollback_trx:" << m_rollback_trx);
      }
    });

    mysql_mutex_lock(&m_mutex);
    thd->ENTER_COND(cond, &m_mutex,
                    &stage_worker_waiting_for_its_turn_to_commit,
                    &old_stage);

    MY_D("Worker "<<worker->id +1 <<" running_status: "<< worker->running_status);
    while (queue_front() != worker->id)
    {
      if (unlikely(worker->found_order_commit_deadlock()))
      {
        mysql_mutex_unlock(&m_mutex);
        thd->EXIT_COND(&old_stage);
        DBUG_RETURN(true);
      }
      MY_D("Worker "<<worker->id +1 << " waiting for signal");
      mysql_cond_wait(cond, &m_mutex);
      MY_D("Worker "<<worker->id +1 << " woken up from cond_wait");
    }
    MY_D("Worker " << worker->id + 1<<" is in the front of the queue, skipping the checks");

    mysql_mutex_unlock(&m_mutex);
    thd->EXIT_COND(&old_stage);

    m_workers[worker->id].status= OCS_SIGNAL;

    if (m_rollback_trx)
    {
      MY_D("Worker "<< worker->id +1 << " has been asked to rollback because old thread encountered an error");
      unregister_trx(worker);

      DBUG_PRINT("info", ("thd has seen an error signal from old thread"));
      thd->get_stmt_da()->set_overwrite_status(true);
      my_error(ER_SLAVE_WORKER_STOPPED_PREVIOUS_THD_ERROR, MYF(0));
    }
  }
  else
    MY_D("Worker "<< worker->id +1 << " failed checks, most probably because it is already signalled");

  MY_D("Worker "<< worker->id +1<< " returning from wait_for_its_turn");
  DBUG_RETURN(m_rollback_trx);
}

void Commit_order_manager::unregister_trx(Slave_worker *worker)
{
  DBUG_ENTER("Commit_order_manager::unregister_trx");

  if (m_workers[worker->id].status == OCS_SIGNAL)
  {
    MY_D("Worker "<< worker->id +1 << " is signalling next transaction");
    DBUG_PRINT("info", ("Worker %lu is signalling next transaction", worker->id));

    mysql_mutex_lock(&m_mutex);

    DBUG_ASSERT(!queue_empty());

    /* Set next manager as the head and signal the trx to commit. */
    queue_pop();
    if (!queue_empty())
      mysql_cond_signal(&m_workers[queue_front()].cond);

    m_workers[worker->id].status= OCS_FINISH;

    mysql_mutex_unlock(&m_mutex);
  }

  DBUG_VOID_RETURN;
}

void Commit_order_manager::report_rollback(Slave_worker *worker)
{
  DBUG_ENTER("Commit_order_manager::report_rollback");

  MY_D("Worker "<< worker->id +1 << " is reporting rollback, entering wait_for_its_turn");
  (void) wait_for_its_turn(worker, true);
  /* No worker can set m_rollback_trx unless it is its turn to commit */
  MY_D("Worker "<< worker->id +1<< " setting m_rollback_trx and unregistering itself");
  m_rollback_trx= true;
  unregister_trx(worker);
  MY_D("Worker queue status after report_rollback:");
  print_queue_status();

  DBUG_VOID_RETURN;
}

void Commit_order_manager::report_deadlock(Slave_worker *worker)
{
  MY_D("Innodb detected that Worker "<< worker->id +1 << " is causing deadlock, setting m_order_commit_deadlock and waking it up if it is in COM::wait");
  DBUG_ENTER("Commit_order_manager::report_deadlock");
  mysql_mutex_lock(&m_mutex);
  worker->report_order_commit_deadlock();
  DBUG_EXECUTE_IF("rpl_fake_cod_deadlock",
                  {
                  const char act[]= "now signal reported_deadlock";
                  DBUG_ASSERT(!debug_sync_set_action(current_thd,
                                                     STRING_WITH_LEN(act)));
                  });
  mysql_cond_signal(&m_workers[worker->id].cond);
  mysql_mutex_unlock(&m_mutex);
  DBUG_VOID_RETURN;
}

