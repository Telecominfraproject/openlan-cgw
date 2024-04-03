use crate::AppArgs;

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
        let serial: MacAddress = row.get("mac");
        let gid: i32 = row.get("infra_group_id");
        Self {
            mac: serial,
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
    pub async fn new(app_args: &AppArgs) -> Self {
        let conn_str = format!(
            "host={host} port={port} user={user} dbname={db} password={pass}",
            host = app_args.db_ip,
            port = app_args.db_port,
            user = app_args.db_username,
            db = app_args.db_name,
            pass = app_args.db_password
        );
        debug!(
            "Trying to connect to remote db ({}:{})...",
            app_args.db_ip.to_string(),
            app_args.db_port.to_string()
        );
        debug!("Conn args {conn_str}");
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        info!("Connected to remote DB");

        CGWDBAccessor { cl: client }
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

    pub async fn insert_new_infra_group(
        &self,
        g: &CGWDBInfrastructureGroup,
    ) -> Result<(), &'static str> {
        let q = self.cl.prepare("INSERT INTO infrastructure_groups (id, reserved_size, actual_size) VALUES ($1, $2, $3)").await.unwrap();
        let res = self
            .cl
            .execute(&q, &[&g.id, &g.reserved_size, &g.actual_size])
            .await;

        match res {
            Ok(_n) => return Ok(()),
            Err(e) => {
                error!(
                    "Failed to insert a new infra group {}: {:?}",
                    g.id,
                    e.to_string()
                );
                return Err("Insert new infra group failed");
            }
        }
    }

    pub async fn delete_infra_group(&self, gid: i32) -> Result<(), &'static str> {
        // TODO: query-base approach instead of static string
        let req = self
            .cl
            .prepare("DELETE FROM infrastructure_groups WHERE id = $1")
            .await
            .unwrap();
        let res = self.cl.execute(&req, &[&gid]).await;

        match res {
            Ok(n) => {
                if n > 0 {
                    return Ok(());
                } else {
                    return Err("Failed to delete group from DB: gid does not exist");
                }
            }
            Err(e) => {
                error!("Failed to delete an infra group {gid}: {:?}", e.to_string());
                return Err("Delete infra group failed");
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
                return Some(list);
            }
            Err(_e) => {
                return None;
            }
        }
    }

    pub async fn get_infra_group(&self, gid: i32) -> Option<CGWDBInfrastructureGroup> {
        let q = self
            .cl
            .prepare("SELECT * from infrastructure_groups WHERE id = $1")
            .await
            .unwrap();
        let row = self.cl.query_one(&q, &[&gid]).await;

        match row {
            Ok(r) => return Some(CGWDBInfrastructureGroup::from(r)),
            Err(_e) => {
                return None;
            }
        }
    }

    /*
    * INFRA db API uses the following table decl
      CREATE TABLE infras (
        mac MACADDR PRIMARY KEY,
        infra_group_id INT,
        FOREIGN KEY(infra_group_id) REFERENCES infrastructure_groups(id) ON DELETE CASCADE
      );
    */

    pub async fn insert_new_infra(&self, infra: &CGWDBInfra) -> Result<(), &'static str> {
        let q = self
            .cl
            .prepare("INSERT INTO infras (mac, infra_group_id) VALUES ($1, $2)")
            .await
            .unwrap();
        let res = self
            .cl
            .execute(&q, &[&infra.mac, &infra.infra_group_id])
            .await;

        match res {
            Ok(_n) => return Ok(()),
            Err(e) => {
                error!("Failed to insert a new infra: {:?}", e.to_string());
                return Err("Insert new infra failed");
            }
        }
    }

    pub async fn delete_infra(&self, serial: MacAddress) -> Result<(), &'static str> {
        let q = self
            .cl
            .prepare("DELETE FROM infras WHERE mac = $1")
            .await
            .unwrap();
        let res = self.cl.execute(&q, &[&serial]).await;

        match res {
            Ok(n) => {
                if n > 0 {
                    return Ok(());
                } else {
                    return Err("Failed to delete infra from DB: MAC does not exist");
                }
            }
            Err(e) => {
                error!("Failed to delete infra: {:?}", e.to_string());
                return Err("Delete infra failed");
            }
        }
    }

    pub async fn get_all_infras(&self) -> Option<Vec<CGWDBInfra>> {
        let mut list: Vec<CGWDBInfra> = Vec::new();

        let res = self
            .cl
            .query("SELECT * from infras", &[])
            .await;

        match res {
            Ok(r) => {
                for x in r {
                    let infra = CGWDBInfra::from(x);
                    list.push(infra);
                }
                return Some(list);
            }
            Err(_e) => {
                return None;
            }
        }
    }
}
