const { workerData, parentPort } = require('worker_threads');
const cassandra = require("cassandra-driver");
const client = new cassandra.Client({
    cloud: {
      secureConnectBundle: "./secure-connect-akathon.zip",
    },
    credentials: {
      username: "SfoYcrqJkkfjHUFtsnCQfUJy",
      password:
        "_6csx1mBIS1QKoEZy+gH.Yb0-YzGLLB9.MJuljuY1gkQtxn,lDu7dg-BDX1Fr8hx52KZ6HnfSAd,vFqShXIL5UhXouGIQukghYzkIoxUKk7NEJ--Z44HpxQLTQGTbsvY",
    },
    keyspace: "akathon",
  });

  async function processData() {
    await client.connect();
    client.connect(function (err) {
        if (err) {
          console.error(err);
        } else {
          console.log("Connected to Astra DB!");
        }
      });
    
    const query = 'SELECT "Game_ID", "Genre", "Name" FROM products;';
    const result = await client.execute(query, { prepare: true});
    let array = result.rows.sort((a, b) => a.Game_ID - b.Game_ID);
    parentPort.postMessage(array);
  }

processData();