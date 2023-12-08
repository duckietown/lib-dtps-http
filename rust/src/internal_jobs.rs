use std::{collections::HashMap, future::Future, pin::Pin, time::Duration};

use tokio::task::JoinHandle;

use crate::{
    debug_with_info, dtpserror_other, error_with_info, server_state::Status, shared_statuses::SharedStatusNotification,
    types::CompositeName, ServerStateAccess, DTPSR,
};

pub type JobFunctionType = Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send>;

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
                        error_with_info!("job {job_name:?} [{desc}]  finished with error: {e:?}");
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
            Self::run_job(
                name2.clone(),
                desc2.clone(),
                job_function,
                restart_on_success,
                restart_on_error,
                backoff,
                ssa,
            )
            .await;
            if restart_on_success || restart_on_error {
                error_with_info!("job {:?}[{desc2}] finished but it should restart", name2.as_dash_sep());
            } else {
                debug_with_info!("job {:?}[{desc2}] finished (no restart)", name2.as_dash_sep());
            }
        });
        let j = InternalJob { handle, desc };
        self.jobs.insert(name.clone(), j);
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
    ) {
        let prefix = format!("job {name:?}[{desc}]:", name = name.as_dash_sep(), desc = desc);
        debug_with_info!("{prefix} handler started");
        let mut handle = job_function();

        loop {
            {
                let mut ss = ssa.lock().await;

                if let Err(e) = ss.send_status_notification(&name, Status::RUNNING, None) {
                    error_with_info!("Error sending status notification: {:?}", e);
                }
            }
            let r = handle.await;
            debug_with_info!("{prefix} finished OK = {}", r.is_ok());
            match r {
                Ok(_) => {
                    let mut ss = ssa.lock().await;
                    if let Err(e) = ss.send_status_notification(&name, Status::EXITED, None) {
                        error_with_info!("{prefix} Error sending status notification: {:?}", e);
                    }

                    if !restart_on_success {
                        break;
                    }
                }
                Err(e) => {
                    let mut ss = ssa.lock().await;
                    if let Err(e2) = ss.send_status_notification(&name, Status::FATAL, Some(e)) {
                        error_with_info!("{prefix} Error sending status notification: {:?}", e2);
                    }

                    if !restart_on_error {
                        break;
                    }
                }
            };

            tokio::time::sleep(Duration::from_secs_f32(backoff)).await;
            handle = job_function();
        }
    }
}
