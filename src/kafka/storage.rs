use std::fs::OpenOptions as StdOpenOptions;
use std::{collections::HashMap, io::Write};

use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};

use crate::utils;

/**
 * storage logs & retrieve logs
 * append:
 *  incr offset by key & store logs
 *
 */
#[derive(Debug)]
pub struct Storage {
    log_name: String,
    // store next offset to this key
    offsets: HashMap<String, usize>,
    log: File,
    node_id: String,
}

impl Drop for Storage {
    /**
     * when drop, save offsets to meta file
     */
    fn drop(&mut self) {
        self.write_meta();
    }
}

impl Storage {
    pub fn offsets(&self) -> &HashMap<String, usize> {
        &self.offsets
    }

    pub fn log_name(&self) -> &str {
        &self.log_name
    }

    pub async fn new(node_id: &str) -> Self {
        let open_options = utils::rw_open_options();
        let filename = format!("{}_log", node_id);
        let path = format!("log/{}", filename);

        let log = open_options.open(path).await.expect("can't open log file");

        Self {
            offsets: Self::load_meta(node_id).await,
            log_name: filename,
            log,
            node_id: node_id.to_string(),
        }
    }

    /**
     * append msg to key, return offset to this msg
     * start from zero
     * with format: offset:key:msg
     */
    pub async fn append(&mut self, key: &str, msg: usize) -> usize {
        let offset = match self.offsets.get_mut(key) {
            Some(offset) => {
                *offset += 1;
                *offset
            }
            None => {
                self.offsets.insert(key.to_owned(), 0);
                0
            }
        };

        self.log
            .write_all(format!("{}:{}:{}", offset, key, msg).as_bytes())
            .await
            .expect("failed to append log");
        self.log
            .write_all("\n".as_bytes())
            .await
            .expect("failed to append newline");

        offset
    }

    /**
     * read log from offsets
     */
    pub async fn read_from(
        &self,
        offsets: &HashMap<String, usize>,
    ) -> HashMap<String, Vec<[usize; 2]>> {
        let mut ret: HashMap<String, Vec<[usize; 2]>> = HashMap::with_capacity(offsets.len());

        let file = utils::r_open_options()
            .open(format!("log/{}", self.log_name))
            .await
            .expect("cannot read log file");

        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            let arr = line.split(':').collect::<Vec<&str>>();
            let offset = arr[0]
                .parse::<usize>()
                .expect("expect offset parse to usize");
            let key = arr[1];
            let msg = arr[2].parse::<usize>().unwrap();

            if let Some(start_offset) = offsets.get(key) {
                if start_offset <= &offset {
                    let values = ret.get_mut(key);
                    match values {
                        Some(v) => v.push([offset, msg]),
                        None => {
                            let v = vec![[offset, msg]];
                            ret.insert(key.to_owned(), v);
                        }
                    }
                }
            }
        }

        ret
    }

    /**
     * write meta data to disk
     * TODO: use tokio library
     */
    fn write_meta(&self) {
        let mut options = StdOpenOptions::new();
        options.read(true).write(true).truncate(true).create(true);

        let mut meta = options.open(Self::meta_filename(&self.node_id)).unwrap();
        meta.write_all(serde_json::to_string(&self.offsets).unwrap().as_bytes())
            .unwrap();
    }

    /**
     * load meta data into offsets
     */
    async fn load_meta(node_id: &str) -> HashMap<String, usize> {
        let options = utils::rw_open_options();
        let meta = options
            .open(Self::meta_filename(node_id))
            .await
            .expect("failed to open meta file");
        let offsets = BufReader::new(meta)
            .lines()
            .next_line()
            .await
            .expect("failed to read meta data");

        match offsets {
            Some(str) => serde_json::from_str(&str).expect("failed to deserialized from meta data"),
            None => HashMap::new(),
        }
    }

    fn meta_filename(node_id: &str) -> String {
        format!("log/{}_meta", node_id)
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use crate::utils::{self, tests::generate_random_node_id};

    use super::Storage;

    pub async fn clean_disk_data(node_id: &str) {
        utils::delete(&format!("log/{}_log", node_id)).await;
        utils::delete(&format!("log/{}_meta", node_id)).await;
    }

    #[tokio::test]
    async fn test_append_and_read() {
        let node_id = generate_random_node_id();
        let mut storage = Storage::new(&node_id).await;
        storage.append("k1", 100).await;
        storage.append("k1", 101).await;
        storage.append("k2", 100).await;
        storage.append("k1", 102).await;
        storage.append("k2", 101).await;
        storage.append("k1", 103).await;

        let read_offsets: HashMap<String, usize> =
            HashMap::from([("k1".to_string(), 1), ("k2".to_string(), 0)]);

        let mut res = storage.read_from(&read_offsets).await;

        let v = res.get_mut("k1").unwrap();
        v.sort_by(|a, b| a[0].cmp(&b[0]));
        assert_eq!(&vec![[1_usize, 101], [2_usize, 102], [3_usize, 103]], v);

        let v = res.get_mut("k2").unwrap();
        v.sort_by(|a, b| a[0].cmp(&b[0]));
        assert_eq!(&vec![[0, 100], [1, 101]], v);

        clean_disk_data(&node_id).await;
    }
}
