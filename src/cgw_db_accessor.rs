use crate::cgw_app_args::CGWDBArgs;

use crate::cgw_tls::cgw_tls_create_db_connect;
use crate::{
    cgw_errors::{Error, Result},
    cgw_metrics::{CGWMetrics, CGWMetricsHealthComponent, CGWMetricsHealthComponentStatus},
};

use eui48::MacAddress;

use tokio_postgres::NoTls;
use tokio_postgres::{row::Row, Client};

#[derive(Clone)]
pub struct CGWDBInfra {
    pub mac: MacAddress,
    pub infra_group_id: i32,
}

#[derive(Clone)]
pub struct CGWDBInfrastructureGroup {
    pub id: i32,
    pub reserved_size: i32,
    pub actual_size: i32,
    pub cloud_header: Option<String>,
}

impl TryFrom<Row> for CGWDBInfra {
    type Error = tokio_postgres::Error;

    fn try_from(row: Row) -> std::result::Result<Self, tokio_postgres::Error> {
        let mac: MacAddress = row.try_get("mac")?;
        let infra_group_id: i32 = row.try_get("infra_group_id")?;
        Ok(Self {
            mac,
            infra_group_id,
        })
    }
}

impl TryFrom<Row> for CGWDBInfrastructureGroup {
    type Error = tokio_postgres::Error;

    fn try_from(row: Row) -> std::result::Result<Self, tokio_postgres::Error> {
        let id: i32 = row.try_get("id")?;
        let reserved_size: i32 = row.try_get("reserved_size")?;
        let actual_size: i32 = row.try_get("actual_size")?;
        Ok(Self {
            id,
            reserved_size,
            actual_size,
            cloud_header: None,
        })
    }
}

pub struct CGWDBAccessor {
    cl: Client,
}

impl CGWDBAccessor {
    pub async fn new(db_args: &CGWDBArgs) -> Result<Self> {
        let conn_str = format!(
            "sslmode={sslmode} host={host} port={port} user={user} dbname={db} password={pass} connect_timeout=10",
            host = db_args.db_host,
            port = db_args.db_port,
            user = db_args.db_username,
            db = db_args.db_name,
            pass = db_args.db_password,
            sslmode = match db_args.db_tls {
                true => "require",
                false => "disable",
            }
        );
        debug!(
            "Trying to connect to remote db ({}:{})...\nConnection args: {}",
            db_args.db_host, db_args.db_port, conn_str
        );

        let client: Client;
        if db_args.db_tls {
            let tls = match cgw_tls_create_db_connect().await {
                Ok(tls_connect) => tls_connect,
                Err(e) => {
                    error!("Failed to build TLS connection with remote DB! Error: {e}");
                    return Err(Error::DbAccessor(
                        "Failed to build TLS connection with remote DB",
                    ));
                }
            };

            let (db_client, connection) = match tokio_postgres::connect(&conn_str, tls).await {
                Ok((cl, conn)) => (cl, conn),
                Err(e) => {
                    error!("Failed to establish connection with DB! Error: {e}");
                    return Err(Error::DbAccessor("Failed to establish connection with DB"));
                }
            };

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    let err_msg = format!("Connection to DB broken! Error: {e}");
                    error!("{}", err_msg);
                    CGWMetrics::get_ref()
                        .change_component_health_status(
                            CGWMetricsHealthComponent::DBConnection,
                            CGWMetricsHealthComponentStatus::NotReady(err_msg),
                        )
                        .await;
                }
            });

            client = db_client;
        } else {
            let (db_client, connection) = match tokio_postgres::connect(&conn_str, NoTls).await {
                Ok((cl, conn)) => (cl, conn),
                Err(e) => {
                    error!("Failed to establish connection with DB! Error: {e}");
                    return Err(Error::DbAccessor("Failed to establish connection with DB"));
                }
            };

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    let err_msg = format!("Connection to DB broken! Error: {e}");
                    error!("{}", err_msg);
                    CGWMetrics::get_ref()
                        .change_component_health_status(
                            CGWMetricsHealthComponent::DBConnection,
                            CGWMetricsHealthComponentStatus::NotReady(err_msg),
                        )
                        .await;
                }
            });

            client = db_client;
        }

        tokio::spawn(async move {
            CGWMetrics::get_ref()
                .change_component_health_status(
                    CGWMetricsHealthComponent::DBConnection,
                    CGWMetricsHealthComponentStatus::Ready,
                )
                .await;
        });

        info!("Connection to SQL DB has been established!");

        Ok(CGWDBAccessor { cl: client })
    }

    /*
    * INFRA_GROUP db API uses the following table decl
    * TODO: id = int, not varchar; requires kafka simulator changes
      CREATE TABLE infrastructure_groups (
        id INT PRIMARY KEY,
        reserved_size INT,
        actual_size INT
      );
    *
    */

    pub async fn insert_new_infra_group(&self, g: &CGWDBInfrastructureGroup) -> Result<()> {
        let q = match self.cl.prepare("INSERT INTO infrastructure_groups (id, reserved_size, actual_size) VALUES ($1, $2, $3)").await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to prepare query (new infra group) for insertion! Error: {e}");
                return Err(Error::DbAccessor("Insert new infra group failed"));
            }
        };
        let res = self
            .cl
            .execute(&q, &[&g.id, &g.reserved_size, &g.actual_size])
            .await;

        match res {
            Ok(_n) => Ok(()),
            Err(e) => {
                error!("Failed to insert a new infra group {}! Error: {}", g.id, e);
                Err(Error::DbAccessor("Insert new infra group failed"))
            }
        }
    }

    pub async fn delete_infra_group(&self, gid: i32) -> Result<()> {
        // TODO: query-base approach instead of static string
        let req = match self
            .cl
            .prepare("DELETE FROM infrastructure_groups WHERE id = $1")
            .await
        {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to prepare query (del infra group) for removal! Error: {e}");
                return Err(Error::DbAccessor("Insert new infra group failed"));
            }
        };
        let res = self.cl.execute(&req, &[&gid]).await;

        match res {
            Ok(n) => {
                if n > 0 {
                    Ok(())
                } else {
                    Err(Error::DbAccessor(
                        "Failed to delete group from DB: gid does not exist",
                    ))
                }
            }
            Err(e) => {
                error!("Failed to delete an infra group {gid}! Error: {e}");
                Err(Error::DbAccessor("Delete infra group failed"))
            }
        }
    }

    pub async fn get_all_infra_groups(&self) -> Option<Vec<CGWDBInfrastructureGroup>> {
        let mut list: Vec<CGWDBInfrastructureGroup> = Vec::with_capacity(1000);

        let res = self
            .cl
            .query("SELECT * from infrastructure_groups", &[])
            .await;

        match res {
            Ok(r) => {
                for x in r {
                    match CGWDBInfrastructureGroup::try_from(x) {
                        Ok(infra_group) => {
                            list.push(infra_group);
                        }
                        Err(e) => {
                            error!("Failed to construct CGWDBInfrastructureGroup! Error: {e}");
                        }
                    }
                }
                Some(list)
            }
            Err(_e) => None,
        }
    }

    pub async fn get_infra_group(&self, gid: i32) -> Option<CGWDBInfrastructureGroup> {
        if let Ok(q) = self
            .cl
            .prepare("SELECT * from infrastructure_groups WHERE id = $1")
            .await
        {
            let row = self.cl.query_one(&q, &[&gid]).await;

            match row {
                Ok(r) => match CGWDBInfrastructureGroup::try_from(r) {
                    Ok(infra_group) => {
                        return Some(infra_group);
                    }
                    Err(e) => {
                        error!("Failed to construct CGWDBInfrastructureGroup! Error: {e}");
                        return None;
                    }
                },
                Err(_e) => {
                    return None;
                }
            };
        }

        None
    }

    /*
    * INFRA db API uses the following table decl
      CREATE TABLE infras (
        mac MACADDR PRIMARY KEY,
        infra_group_id INT,
        FOREIGN KEY(infra_group_id) REFERENCES infrastructure_groups(id) ON DELETE CASCADE
      );
    */

    pub async fn insert_new_infra(&self, infra: &CGWDBInfra) -> Result<()> {
        let q = match self
            .cl
            .prepare("INSERT INTO infras (mac, infra_group_id) VALUES ($1, $2)")
            .await
        {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to insert new infra! Error: {e}");
                return Err(Error::DbAccessor("Failed to insert new infra"));
            }
        };
        let res = self
            .cl
            .execute(&q, &[&infra.mac, &infra.infra_group_id])
            .await;

        match res {
            Ok(_n) => Ok(()),
            Err(e) => {
                error!("Failed to insert new infra! Error: {e}");
                Err(Error::DbAccessor("Insert new infra failed"))
            }
        }
    }

    pub async fn delete_infra(&self, serial: MacAddress) -> Result<()> {
        let q = match self.cl.prepare("DELETE FROM infras WHERE mac = $1").await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to delete infra! Error: {e}");
                return Err(Error::DbAccessor("Failed to delete infra from DB"));
            }
        };
        let res = self.cl.execute(&q, &[&serial]).await;

        match res {
            Ok(n) => {
                if n > 0 {
                    Ok(())
                } else {
                    Err(Error::DbAccessor(
                        "Failed to delete infra from DB: MAC does not exist!",
                    ))
                }
            }
            Err(e) => {
                error!("Failed to delete infra! Error: {e}");
                Err(Error::DbAccessor("Delete infra failed"))
            }
        }
    }

    pub async fn get_all_infras(&self) -> Option<Vec<CGWDBInfra>> {
        let mut list: Vec<CGWDBInfra> = Vec::new();

        let res = self.cl.query("SELECT * from infras", &[]).await;

        match res {
            Ok(r) => {
                for x in r {
                    match CGWDBInfra::try_from(x) {
                        Ok(infra) => {
                            list.push(infra);
                        }
                        Err(e) => {
                            error!("Failed to construct CGWDBInfra! Error: {e}");
                        }
                    }
                }
                Some(list)
            }
            Err(e) => {
                error!("Failed to retrieve infras from DB! Error: {e}");
                None
            }
        }
    }

    pub async fn get_group_infras(&self, group_id: i32) -> Option<Vec<CGWDBInfra>> {
        let mut list: Vec<CGWDBInfra> = Vec::new();

        match self
            .cl
            .prepare("SELECT * from infras WHERE infra_group_id = $1")
            .await
        {
            Ok(q) => {
                match self.cl.query(&q, &[&group_id]).await {
                    Ok(r) => {
                        for x in r {
                            match CGWDBInfra::try_from(x) {
                                Ok(infra) => {
                                    list.push(infra);
                                }
                                Err(e) => {
                                    error!("Failed to construct CGWDBInfra! Error: {e}");
                                }
                            }
                        }
                        return Some(list);
                    }
                    Err(e) => {
                        error!("Query infras with group id {group_id} failed! Error: {e}");
                        return None;
                    }
                };
            }
            Err(e) => {
                error!("Failed to prepare statement! Error: {e}");
                return None;
            }
        }
    }

    pub async fn get_infra(&self, mac: MacAddress) -> Option<CGWDBInfra> {
        match self.cl.prepare("SELECT * from infras WHERE mac = $1").await {
            Ok(q) => {
                let row = self.cl.query_one(&q, &[&mac]).await;
                if let Ok(r) = row {
                    match CGWDBInfra::try_from(r) {
                        Ok(infra) => {
                            return Some(infra);
                        }
                        Err(e) => {
                            error!("Failed to construct CGWDBInfra! Error: {e}");
                            return None;
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to prepare statement! Error: {e}");
                return None;
            }
        }

        None
    }
}
