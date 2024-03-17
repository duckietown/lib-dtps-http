use std::{collections::HashMap, future::Future, pin::Pin, time::Duration};

use tokio::task::JoinHandle;

use crate::{
    debug_with_info, dtpserror_other, error_with_info, server_state::Status, shared_statuses::SharedStatusNotification,
    types::CompositeName, DTPSError, ServerStateAccess, DTPSR,
};

pub type JobFunctionType = Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), DTPSError>> + Send>> + Send>;

#[derive(Debug)]
pub struct InternalJob {
    handle: JoinHandle<()>,
    desc: String,
}

#[derive(Debug)]
pub struct InternalJobManager {
    jobs: HashMap<CompositeName, InternalJob>,
}

impl InternalJobManager {
    pub fn new() -> Self {
        Self { jobs: HashMap::new() }
    }

    pub fn remove_job(&mut self, name: &CompositeName) -> DTPSR<SharedStatusNotification> {
        if let Some(job) = self.jobs.remove(name) {
            job.handle.abort();
            let desc = format!("job {name:?} [{desc}]", name = name.as_dash_sep(), desc = job.desc)
                .as_str()
                .to_string();
            let event = SharedStatusNotification::new(&desc);
            let event2 = event.clone();
            let job_name = name.to_dash_sep();
            let desc = job.desc.clone();
            tokio::spawn(async move {
                match job.handle.await {
                    Ok(_) => {
                        debug_with_info!("job {job_name:?} [{desc}] finished OK");
                        event2.notify(true, "Job finished").await.unwrap();
                    }
                    Err(e) => {
                        if !e.is_cancelled() {
                            error_with_info!("job {job_name:?} [{desc}] finished with error: {e:?}");
                        }
                        event2.notify(true, "Job finished").await.unwrap();
                    }
                }
            });

            Ok(event)
        } else {
            let s = format!(
                "Job {} not found: known {}",
                name.as_dash_sep(),
                self.jobs.keys().map(|x| x.as_dash_sep()).collect::<Vec<_>>().join(", ")
            );
            dtpserror_other!("{}", s)
        }
    }
    pub fn add_job(
        &mut self,
        name: &CompositeName,
        desc: &str,
        job_function: JobFunctionType,
        restart_on_success: bool,
        restart_on_error: bool,
        backoff: f32,
        ssa: ServerStateAccess,
    ) -> DTPSR<()> {
        let desc = desc.to_string();
        let desc2 = desc.clone();
        let name2 = name.clone();
        let handle = tokio::spawn(async move {
            let name2 = name2.clone();
            debug_with_info!("job {:?}[{desc2}] starting", name2.as_dash_sep());
            let ok = Self::run_job(
                name2.clone(),
                desc2.clone(),
                job_function,
                restart_on_success,
                restart_on_error,
                backoff,
                ssa,
            )
            .await;

            if ok.is_ok() && restart_on_success {
                error_with_info!("job {:?}[{desc2}] finished but it should restart", name2.as_dash_sep());
            }

            if ok.is_err() && restart_on_error {
                error_with_info!(
                    "job {:?}[{desc2}] finished with error but it should restart",
                    name2.as_dash_sep()
                );
            }
        });
        let j = InternalJob { handle, desc };
        self.jobs.insert(name.clone(), j);
        Ok(())
    }
    pub async fn stop_all_jobs(&mut self) -> DTPSR<()> {
        for (c, j) in self.jobs.drain() {
            let h = j.handle;
            h.abort();

            assert!(h.await.unwrap_err().is_cancelled());
        }
        Ok(())
    }
    pub async fn run_job(
        name: CompositeName,
        desc: String,
        job_function: JobFunctionType,
        restart_on_success: bool,
        restart_on_error: bool,
        backoff: f32,
        ssa: ServerStateAccess,
    ) -> DTPSR<()> {
        let prefix = format!("job {name:?} [{desc}]:", name = name.as_dash_sep(), desc = desc);
        debug_with_info!("{prefix} handler started");
        let mut handle = job_function();
        let mut iteration = -1;
        loop {
            iteration += 1;
            {
                let mut ss = ssa.lock().await;

                if let Err(e) = ss.send_status_notification(&name, Status::RUNNING, None) {
                    error_with_info!("Error sending status notification:\n{:?}", e);
                }
            }
            let r = handle.await;
            debug_with_info!("{prefix} finished OK = {}", r.is_ok());

            match &r {
                Ok(_) => {
                    // debug_with_info!("Iteration {iteration} of {prefix} finished OK");

                    let mut ss = ssa.lock().await;
                    if let Err(e) = ss.send_status_notification(&name, Status::EXITED, None) {
                        error_with_info!("{prefix} Error sending status notification:\n{:?}", e);
                    }

                    if !restart_on_success {
                        return Ok(());
                    }
                }
                Err(e) => {
                    let s = e.to_string();
                    error_with_info!("Iteration {iteration} of {prefix} finished with error:\n{s}");

                    let mut ss = ssa.lock().await;
                    if let Err(e2) = ss.send_status_notification(&name, Status::FATAL, Some(e.to_string())) {
                        // XXX
                        error_with_info!("{prefix} Error sending status notification:\n{:?}", e2);
                    }

                    if !restart_on_error {
                        return r;
                    }
                }
            };
            debug_with_info!("{prefix} backing off for {backoff} seconds and restarting");

            tokio::time::sleep(Duration::from_secs_f32(backoff)).await;
            handle = job_function();
        }
    }
}
