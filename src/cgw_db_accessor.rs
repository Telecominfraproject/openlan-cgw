use crate::cgw_app_args::CGWDBArgs;

use crate::{
    cgw_errors::{Error, Result},
    cgw_metrics::{CGWMetrics, CGWMetricsHealthComponent, CGWMetricsHealthComponentStatus},
};

use eui48::MacAddress;

use tokio_postgres::{row::Row, Client, NoTls};

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
}

impl From<Row> for CGWDBInfra {
    fn from(row: Row) -> Self {
        let mac: MacAddress = row.get("mac");
        let gid: i32 = row.get("infra_group_id");
        Self {
            mac,
            infra_group_id: gid,
        }
    }
}

impl From<Row> for CGWDBInfrastructureGroup {
    fn from(row: Row) -> Self {
        let infra_id: i32 = row.get("id");
        let res_size: i32 = row.get("reserved_size");
        let act_size: i32 = row.get("actual_size");
        Self {
            id: infra_id,
            reserved_size: res_size,
            actual_size: act_size,
        }
    }
}

pub struct CGWDBAccessor {
    cl: Client,
}

impl CGWDBAccessor {
    pub async fn new(db_args: &CGWDBArgs) -> Result<Self> {
        let conn_str = format!(
            "host={host} port={port} user={user} dbname={db} password={pass} connect_timeout=10",
            host = db_args.db_host,
            port = db_args.db_port,
            user = db_args.db_username,
            db = db_args.db_name,
            pass = db_args.db_password
        );
        debug!(
            "Trying to connect to remote db ({}:{})...\nConn args {}",
            db_args.db_host, db_args.db_port, conn_str
        );

        let (client, connection) = match tokio_postgres::connect(&conn_str, NoTls).await {
            Ok((cl, conn)) => (cl, conn),
            Err(e) => {
                error!("Failed to establish connection with DB, reason: {:?}", e);
                return Err(Error::DbAccessor("Failed to establish connection with DB"));
            }
        };

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                let err_msg = format!("Connection to DB broken: {}", e);
                error!("{}", err_msg);
                CGWMetrics::get_ref()
                    .change_component_health_status(
                        CGWMetricsHealthComponent::DBConnection,
                        CGWMetricsHealthComponentStatus::NotReady(err_msg),
                    )
                    .await;
            }
        });

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
                error!("Failed to prepare query (new infra group) for insertion, reason: {:?}", e);
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
                error!(
                    "Failed to insert a new infra group {}: {:?}",
                    g.id,
                    e.to_string()
                );
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
                error!(
                    "Failed to prepare query (del infra group) for removal, reason: {:?}",
                    e
                );
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
                error!("Failed to delete an infra group {gid}: {:?}", e.to_string());
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
                    let infra_group = CGWDBInfrastructureGroup::from(x);
                    list.push(infra_group);
                }
                Some(list)
            }
            Err(_e) => None,
        }
    }

    #[allow(dead_code)]
    pub async fn get_infra_group(&self, gid: i32) -> Option<CGWDBInfrastructureGroup> {
        if let Ok(q) = self
            .cl
            .prepare("SELECT * from infrastructure_groups WHERE id = $1")
            .await
        {
            let row = self.cl.query_one(&q, &[&gid]).await;

            match row {
                Ok(r) => Some(CGWDBInfrastructureGroup::from(r)),
                Err(_e) => return None,
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
                error!("Failed to insert new infra, reason: {:?}", e);
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
                error!("Failed to insert a new infra: {:?}", e.to_string());
                Err(Error::DbAccessor("Insert new infra failed"))
            }
        }
    }

    pub async fn delete_infra(&self, serial: MacAddress) -> Result<()> {
        let q = match self.cl.prepare("DELETE FROM infras WHERE mac = $1").await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to delete infra, reason: {:?}", e);
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
                        "Failed to delete infra from DB: MAC does not exist",
                    ))
                }
            }
            Err(e) => {
                error!("Failed to delete infra: {:?}", e.to_string());
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
                    let infra = CGWDBInfra::from(x);
                    list.push(infra);
                }
                Some(list)
            }
            Err(e) => {
                error!("Failed to retrieve infras from DB, reason: {:?}", e);
                None
            }
        }
    }
}
