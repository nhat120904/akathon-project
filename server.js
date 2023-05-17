const express = require("express");
const cassandra = require("cassandra-driver");
const bodyParser = require("body-parser");
const { Worker } = require("worker_threads");
const runWorkerThread = require("./worker");
const jwt = require("jsonwebtoken");
const app = express();
const PORT = 3000;
app.use(express.json());
app.use(express.static("public"));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.disable("x-powered-by");
const cookieParser = require("cookie-parser");
app.use(cookieParser());
const bcrypt = require("bcryptjs");
const salt = bcrypt.genSaltSync(10);
const { v4: uuidv4, parse: uuidParse } = require("uuid");
app.disable("x-powered-by");
app.use(express.json());
app.use((req, res, next) => {
    res.setHeader("Access-Control-Allow-Origin", "http://localhost:5173");
    res.setHeader("Access-Control-Allow-Credentials", true);
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader(
        "Access-Control-Allow-Headers",
        "Content-Type, Authorization"
    );
    next();
});
const jwtSecret =
    "f65f59063ae500c9811af98538d05e8668f377c973895c13c7fcd10ae21fe6e3d25376";

//Cassandra config
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

async function checkUser(username, email) {
    const query1 = `SELECT * FROM users WHERE username = ?;`;
    const query2 = `SELECT * FROM users WHERE email = ?;`;

    const [result1, result2] = await Promise.all([
        client.execute(query1, [username], { prepare: true }),
        client.execute(query2, [email], { prepare: true }),
    ]);
    console.log(result1.rowLength);
    console.log(result2.rowLength);

    if (result1.rowLength > 0 && result2.rowLength > 0) {
        const row1 = result1.first();
        const row2 = result2.first();
        console.log("Username from DB: ", row1.username);
        console.log("Email from DB: ", row2.email);
        if (row1.email === email && row2.username === username) {
            // user already exists with same email and username
            return { result: false, content: "username and email" };
        } else if (row1.email === email) {
            // user already exists with same email
            return { result: false, content: "email" };
        } else {
            // user already exists with same username
            return { result: false, content: "username" };
        }
    } else if (result2.rowLength > 0) {
        const row = result2.first();
        // console.log("Email from DB: ", row.email);
        if (row.email === email) {
            // user already exists with same email
            return { result: false, content: "email" };
        } else {
            // user does not exist
            return { result: true, content: "" };
        }
    } else if (result1.rowLength > 0) {
        const row = result1.first();
        console.log("Username from DB: ", row.username);
        if (row.username === username) {
            // user already exists with same username
            return { result: false, content: "username" };
        } else {
            // user does not exist
            return { result: true, content: "" };
        }
    } else {
        // user does not exist
        return { result: true, content: "" };
    }
}

async function checkUserSignIn(account, password, is_user) {
    const key = is_user ? "username" : "email";
    const query = `SELECT * FROM users WHERE ${key} = ?;`;
    const params = [account];
    const result = await client.execute(query, params, { prepare: true });

    // Check if the user exists and the password matches
    if (
        result.rows.length > 0 &&
        bcrypt.compareSync(password, result.rows[0].password)
    ) {
        return { result: true, data: result.rows[0] }; // successful login
    } else {
        return { result: false }; // login failed
    }
}

async function DeleteUser(username) {
    const query1 = `SELECT * FROM users WHERE username = ?;`;
    const params1 = [username];
    const result1 = await client.execute(query1, params1, { prepare: true });

    // console.log(result1)
    if (result1.rowLength === 0) {
        return { result: false };
    }
    const user_id = result1.rows[0].user_id;
    const query = `DELETE FROM users WHERE user_id = ?`;
    const params = [user_id];
    await client.execute(query, params, { prepare: true });
    return { result: true };
}

async function startServer() {
    await client.connect();
    const array = await runWorkerThread();
    client.connect(function (err) {
        if (err) {
            console.error(err);
        } else {
            console.log("Connected to Astra DB!");
        }
    });

    function verifyToken(req, res, next) {
        // get cookieToken
        const token = req.cookies.jwt;
        if (token !== undefined) {
            jwt.verify(token, jwtSecret, (err, data) => {
                if (err) {
                    res.json({
                        result: false,
                        code: 1,
                    }); // forbidden
                } else {
                    req.token = token;
                    req.auth = data;
                    next();
                }
            });
        } else {
            res.json({
                result: false,
                code: 1,
            }); // forbidden
        }
    }

    app.get("/isuser", async (req, res) => {
        const user_id = req.cookies.user_id;
        const token = req.cookies.jwt;
        if (!user_id && !token) {
            res.status(200).json({
                result: true,
                data: "guest",
            });
        } else {
            if (verifyToken_bool(token)) {
                const query = `SELECT is_admin FROM users WHERE user_id = ${user_id}`;
                client
                    .execute(query, { prepare: true })
                    .then((result) => {
                        if (result.rowLength === 0) {
                            res.status(401).json({
                                result: false,
                                code: 2, // user not exist
                            });
                        }
                        if (result.rows[0].is_admin === true) {
                            res.status(200).json({
                                result: true,
                                data: "admin",
                            });
                        }
                        if (
                            result.rows[0].is_admin === false ||
                            result.rows[0].is_admin === null
                        ) {
                            res.status(200).json({
                                result: true,
                                data: "user",
                            });
                        }
                    })
                    .catch((err) => {
                        console.error(err);
                        res.status(401).json({
                            result: false,
                            code: 3, // error in server, or database
                        });
                    });
            } else {
                res.status(401).json({
                    result: false,
                    code: 1, // user exist but token expired or invalid
                });
            }
        }
    });

    // verify token, but return boolean and only take jwt as parameter
    function verifyToken_bool(token) {
        return jwt.verify(token, jwtSecret, (err, _) => {
            if (err) {
                console.log(err.message);
                return false;
            } else {
                return true;
            }
        });
    }

    // get number of games
    app.get("/getgame/number", async (req, res) => {
        const query = `SELECT COUNT(*) FROM products;`;
        client.execute(query, (err, result) => {
            if (err) {
                console.error(err);
                res.json({
                    result: false,
                    code: 3,
                });
            } else {
                res.json({
                    result: true,
                    data: result.rows[0],
                });
            }
        });
    });

    // get all games
    app.get("/get", async (req, res) => {
        const req_index = parseInt(req.query.startIndex);
        let startIndex = 1;
        if (req_index > 0) {
            startIndex = req_index;
        }
        const id = Array.from({ length: 4 }, (_, i) => i + startIndex);
        const query =
            'SELECT "Game_ID", "Genre", "Price", "Rate", "Image_path", "Name", "Subtitle" FROM products WHERE "Game_ID" IN (?, ?, ?, ?) ALLOW FILTERING;';
        client.execute(query, id, { prepare: true }, (err, result) => {
            if (err) {
                console.error(err);
                res.json({
                    result: false,
                    code: 3,
                });
            } else {
                res.json({
                    result: true,
                    data: result.rows.sort((a, b) => a.Game_ID - b.Game_ID),
                });
            }
        });
    });

    // get game by Game_ID
    app.get("/get/:id", async (req, res) => {
        const id = req.params.id;
        console.log(id);
        const query = `SELECT * FROM products WHERE "Game_ID" = ${id};`;
        client.execute(query, (err, result) => {
            if (err) {
                console.error(err);
                res.json({
                    result: false,
                    code: 3,
                });
            } else {
                res.json({
                    result: true,
                    data: result.rows[0],
                });
            }
        });
    });

    app.get("/library", verifyToken, async (req, res) => {
        const query = `SELECT games FROM users WHERE user_id = ${req.cookies.user_id};`;
        try {
            const result = await client.execute(query, { prepare: true });
            const games = result.rows[0].games;
            part2(games);
        } catch (e) {
            console.error(e);
            res.json({
                result: false,
                code: 3,
            });
        }
        function part2(games) {
            if (games === null) {
                res.json({
                    result: true,
                    data: [],
                });
            } else {
                let game = Array.from(
                    { length: games.length },
                    (_) => "?"
                ).join(",");
                console.log(game);
                const query2 = `SELECT "Game_ID", "Image_path", "Name" FROM products WHERE "Game_ID" IN (${game}) ALLOW FILTERING;`;
                client.execute(
                    query2,
                    games,
                    { prepare: true },
                    (err, result) => {
                        if (err) {
                            console.error(err);
                            res.json({
                                result: false,
                                code: 3,
                            });
                        } else {
                            res.json({
                                result: true,
                                data: result.rows.sort(
                                    (a, b) => a.Game_ID - b.Game_ID
                                ),
                            });
                        }
                    }
                );
            }
        }
    });

    // get game by Game_ID, payment version. Call for one game only
    app.get("/get/:id/payment", async (req, res) => {
        const id = req.params.id;
        const query = `SELECT "Game_ID", "Image_path", "Name", "Price" FROM products WHERE "Game_ID" = ${id};`;
        client.execute(query, (err, result) => {
            if (err) {
                console.error(err);
                res.json({
                    result: false,
                    code: 3,
                });
            } else {
                res.json({
                    result: true,
                    data: result.rows[0],
                });
            }
        });
    });

    // get game by Game_ID, payment version. Multiple games
    app.post("/get/payment", async (req, res) => {
        console.log(req.body.data); // [1, 2, 3, 4]
        const id = req.body.data.join(",");
        const query = `SELECT "Game_ID", "Image_path", "Name", "Price" FROM products WHERE "Game_ID" IN (${id}) ALLOW FILTERING;`;
        client.execute(query, (err, result) => {
            if (err) {
                console.error(err);
                res.json({
                    result: false,
                    code: 3,
                });
            } else {
                res.json({
                    result: true,
                    data: result.rows.sort((a, b) => a.Game_ID - b.Game_ID),
                });
            }
        });
    });

    // add review
    app.post("/games/:game_id/reviews", verifyToken, async (req, res) => {
        const rating = req.body.rating;
        const review = req.body.review;
        const user_id = req.cookies.user_id;
        const { game_id } = req.params;
        const query2 = `SELECT MAX(comment_id) FROM game_reviews WHERE game_id = ?;`;
        const data2_async = client.execute(query2, [game_id], {
            prepare: true,
        });
        const data2 = await data2_async;
        let comments = data2.rows[0]["system.max(comment_id)"];
        let comment_id = parseInt(comments);
        comment_id = comment_id.toString() === "NaN" ? 1 : comment_id + 1;

        // Insert the review into the game_reviews table
        const query = `INSERT INTO game_reviews (game_id, user_id, rating, review, comment_id, review_time) VALUES (?, ?, ?, ?, ?, totimestamp(now()));`;
        const params = [game_id, user_id, rating, review, comment_id];
        client
            .execute(query, params, { prepare: true })
            .then((_) => {
                res.json({
                    result: true,
                    data: comment_id,
                });
            })
            .catch((error) => {
                res.json({
                    result: false,
                    code: 2,
                });
            });
    });

    // get reviews
    app.get("/games/:game_id/reviews", (req, res) => {
        const { game_id } = req.params;
        let comment_id = req.query.comment_id;
        if (comment_id === undefined) {
            comment_id = 0;
        }

        // Retrieve the reviews for the specified game
        const query = `SELECT comment_id, rating, review, review_time FROM game_reviews WHERE game_id = ? AND comment_id > ? LIMIT 4;`;
        const params = [game_id, parseInt(comment_id)];
        client
            .execute(query, params, { prepare: true })
            .then((result) => {
                res.status(200).json({
                    result: true,
                    data: result.rows,
                });
            })
            .catch((e) => {
                console.log(e);
                res.status(500).json({
                    result: false,
                    code: 1,
                });
            });
    });

    // get total number of reviews for a game
    app.get("/games/:game_id/reviews/count", (req, res) => {
        const { game_id } = req.params;
        const query = `SELECT COUNT(*) FROM game_reviews WHERE game_id = ${game_id};`;
        client
            .execute(query, { prepare: true })
            .then((result) => {
                res.status(200).json({
                    result: true,
                    data: result.rows[0].count,
                });
            })
            .catch((e) => {
                console.log(e);
                res.status(500).json({
                    result: false,
                    code: 1,
                });
            });
    });

    //get user info
    app.get("/user", verifyToken, (req, res) => {
        const query = `SELECT firstname, lastname, address, email, phone, games, username FROM users WHERE user_id = ${req.cookies.user_id};`;
        client.execute(query, { prepare: true }, (err, result) => {
            if (err) {
                console.error(err);
                res.json({
                    result: false,
                });
            } else {
                res.json({
                    result: true,
                    data: result.rows[0],
                });
            }
        });
    });

    // get only username
    app.get("/user/username", verifyToken, (req, res) => {
        const query = `SELECT username FROM users WHERE user_id = ${req.cookies.user_id};`;
        client.execute(query, { prepare: true }, (err, result) => {
            if (err) {
                console.log(err);
                res.json({
                    result: false,
                });
            } else {
                res.json({
                    result: true,
                    data: result.rows[0],
                });
            }
        });
    });

    //sign up
    app.post("/signup", async (req, res) => {
        const user_id = uuidv4();
        const firstname = req.body.firstname;
        const lastname = req.body.lastname;
        const phone = req.body.phonenumber;
        const address = req.body.address;
        const username = req.body.username;
        const salt = await bcrypt.genSalt(10);
        const hashedPassword = await bcrypt.hash(req.body.password, salt);
        const email = req.body.email;
        const games = [];
        const userExists = await checkUser(username, email);
        if (userExists.result === true) {
            const query = `INSERT INTO users (user_id, firstname, lastname, phone, address, username, password, email, games, is_admin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, false);`;
            client
                .execute(
                    query,
                    [
                        user_id,
                        firstname,
                        lastname,
                        phone,
                        address,
                        username,
                        hashedPassword,
                        email,
                        games,
                    ],
                    { prepare: true }
                )
                .then(() => {
                    const maxAge = 6 * 60 * 60;
                    const token = jwt.sign({ username }, jwtSecret, {
                        expiresIn: maxAge, // 6hrs in sec
                    });
                    res.cookie("jwt", token, {
                        httpOnly: true,
                        maxAge: maxAge * 1000, // 6hrs in ms
                    });
                    res.cookie("user_id", user_id, {
                        httpOnly: true,
                        maxAge: maxAge * 1000, // 6hrs in ms
                    });
                    res.status(200).json({ result: true, data: user_id });
                })
                .catch((err) => {
                    console.error(err);
                    res.json({
                        result: false,
                        code: 0,
                    });
                });
        } else {
            if (userExists.content === "username") {
                res.json({
                    result: false,
                    code: 1,
                });
            }
            if (userExists.content === "email") {
                res.json({
                    result: false,
                    code: 2,
                });
            }
        }
    });

    //sign in
    app.post("/signin", async (req, res) => {
        const is_user = req.body.is_username;
        const password = req.body.password;

        const userExists = await checkUserSignIn(
            req.body.email_or_username,
            password,
            is_user
        );
        console.log(userExists);

        if (userExists.result === true) {
            const user_id = userExists.data.user_id;
            const is_admin = userExists.data.is_admin;
            console.log(user_id);
            const username = userExists.data.username;
            const maxAge = 6 * 60 * 60;
            const token = jwt.sign({ username }, jwtSecret, {
                expiresIn: maxAge, // 6hrs in sec
            });
            res.cookie("jwt", token, {
                httpOnly: true,
                maxAge: maxAge * 1000, // 6hrs in ms
            });
            res.cookie("user_id", user_id, {
                httpOnly: true,
                maxAge: maxAge * 1000, // 6hrs in ms
            });
            res.status(201).json({
                result: true,
                data: {
                    user_id,
                    is_admin,
                },
            });
        } else {
            res.json({
                result: false,
            });
        }
    });

    //admin
    app.post("/admin", async (req, res) => {
        const user_name = req.body.user_name;
        const admin = true;

        if (admin === true) {
            const result = await DeleteUser(user_name);
            if (result.result === false) {
                res.status(404).json({
                    message: "User not found",
                    result: false,
                });
            } else {
                res.status(200).json({
                    message: "User successfully deleted",
                    result: true,
                });
            }
        } else {
            res.status(401).json({
                message: "Unauthorized access",
                result: false,
            });
        }
    });

    // get first 16 usernames
    app.get("/admin/usernames", verifyToken, async (req, res) => {
        const query = `SELECT username FROM users WHERE is_admin = false LIMIT 16;`;
        try {
            const result = await client.execute(query, { prepare: true });
            res.status(200).json({
                result: true,
                data: result.rows,
            });
        } catch (e) {
            console.log(e);
            res.status(500).json({
                result: false,
            });
        }
    });

    // search for a user
    app.post("/admin/search", verifyToken, async (req, res) => {
        const username = req.body.username;
        const query = `SELECT username FROM users WHERE username = ? AND is_admin = false;`;
        try {
            const result = await client.execute(query, [username], {
                prepare: true,
            });
            if (result.rowLength === 0) {
                res.status(404).json({
                    result: false,
                });
            } else {
                res.status(200).json({
                    result: true,
                    data: result.rows[0],
                });
            }
        } catch (e) {
            console.log(e);
            res.status(500).json({
                result: false,
            });
        }
    });

    // sign out
    app.post("/signout", async (req, res) => {
        const token = req.cookies.jwt;
        const user_id = req.cookies.user_id;
        if (!token && !user_id) {
            res.status(401).json({
                result: false,
            });
            return;
        } else {
            res.clearCookie("user_id");
            res.clearCookie("jwt");
            res.status(200).json({
                result: true,
            });
        }
    });

    //payments
    app.post("/order", verifyToken, async (req, res) => {
        const cardName = req.body.data.card_name;
        const cardNumber = req.body.data.card_number;
        const cardType = req.body.data.typeof_card;
        const CVV = req.body.data.cvv;
        const expDate = req.body.data.exp_date;
        const game_id = req.body.data.game_id;
        const price = req.body.data.price;
        const user_id = req.cookies.user_id;
        const query =
            "INSERT INTO orders (user_id, order_id, cardtype, cardname, expdate, cvv, cardNumber, price, game_id, ordertime) VALUES (?,?,?,?,?,?,?,?,?,toTimestamp(now())) ";
        const param = [
            user_id,
            uuidv4(),
            cardType,
            cardName,
            expDate,
            CVV,
            cardNumber,
            price,
            game_id,
        ];
        client.execute(query, param, { prepare: true }, async (err, result) => {
            if (err) {
                console.error(err);
                res.json({
                    result: false,
                    code: 1,
                });
            } else {
                console.log("data received");
                await updateGame(game_id, user_id);
            }
        });
        async function updateGame(game_id, user_id) {
            let exist = await getGames(user_id);
            if (exist === false) {
                res.json({
                    result: false,
                    code: 2,
                });
            } else {
                exist = exist === null ? [] : exist;
                const query = "UPDATE users SET games = ? where user_id = ?;";
                const param = [exist.concat(game_id), user_id];
                client.execute(
                    query,
                    param,
                    { prepare: true },
                    (err, result) => {
                        if (err) {
                            console.error(err);
                            res.json({
                                result: false,
                                code: 1,
                            });
                        } else {
                            console.log("data received");
                            res.json({
                                result: true,
                            });
                        }
                    }
                );
            }
        }
        async function getGames(i) {
            const query = "SELECT games FROM users WHERE user_id = ?;";
            try {
                const result = await client.execute(query, [i], {
                    prepare: true,
                });
                return result.rows[0].games;
            } catch (e) {
                console.error(e);
                return false;
            }
        }
    });

    //statistics
    app.get("/statistics", async (req, res) => {
        const query1 = "SELECT COUNT(*) FROM users";
        const query2 = "SELECT COUNT(*) FROM game_reviews";
        const query3 = "SELECT price FROM orders";
        const data1_async = client.execute(query1, { prepare: true });
        const data2_async = client.execute(query2, { prepare: true });
        const data3_async = client.execute(query3, { prepare: true });
        const [data1, data2, data3] = await Promise.all([
            data1_async,
            data2_async,
            data3_async,
        ]);
        res.json({
            users: data1.rows[0].count,
            comments: data2.rows[0].count,
            money: parseFloat(
                data3.rows.reduce((sum, row) => sum + row.price, 0).toFixed(2)
            ),
        });
    });

    app.get("/rating/avg", async (req, res) => {
        const game_id = Number(req.query.game_id);
        const query  = `SELECT AVG(rating) FROM game_reviews WHERE game_id = ${game_id.toString() === "NaN" ? 0 : game_id}`;
        const data = await client.execute(query, { prepare: true });
        res.json({
            result: true,
            data: data.rows[0]["system.avg(rating)"],
        });
    });

    app.get("/statistics/star/:id", async (req, res) => {
        const id = req.params.id;
        if (isNaN(Number(id))) {
            res.json({
                result: false,
                code: 1,
            });
            return;
        }
        const query = "SELECT rating FROM game_reviews WHERE game_id = ?;";
        const query2 = `SELECT "Name" FROM products WHERE "Game_ID" = ?;`;
        const data_async = client.execute(query, [id], { prepare: true });
        const data2_async = client.execute(query2, [id], { prepare: true });
        const [data, data2] = await Promise.all([data_async, data2_async]);
        const result = data.rows.reduce((acc, row) => {
            if (acc[row.rating]) {
                acc[row.rating]++;
            } else {
                acc[row.rating] = 1;
            }
            return acc;
        }, {});
        for (let i = 1; i <= 5; i++) {
            if (!result[i]) {
                result[i] = 0;
            }
        }
        res.json({
            result: true,
            data: result,
            data2: data2.rows[0].Name,
        });
    });

    //games search
    app.post("/searching", async (req, res) => {
        const data = req.body.data;
        try {
            if (data.length === 2) {
                var name = data[0].value;
                var genre = data[1].value;
            } else {
                var key = data[0].key;
                if (key === "name") {
                    var name = data[0].value;
                    var genre = undefined;
                } else {
                    var name = undefined;
                    var genre = data[0].value;
                }
            }
            let result2 = [];
            if (genre === undefined) {
                for (let i = 0; i < array.length; i++) {
                    if (
                        array[i].Name.toLowerCase().includes(name.toLowerCase())
                    ) {
                        result2.push(array[i].Game_ID);
                    }
                }
            } else if (name === undefined) {
                for (let i = 0; i < array.length; i++) {
                    if (
                        array[i].Genre.toLowerCase().includes(
                            genre.toLowerCase()
                        )
                    ) {
                        result2.push(array[i].Game_ID);
                    }
                }
            } else {
                for (let i = 0; i < array.length; i++) {
                    if (
                        array[i].Name.toLowerCase().includes(
                            name.toLowerCase()
                        ) &&
                        array[i].Genre.toLowerCase().includes(
                            genre.toLowerCase()
                        )
                    ) {
                        result2.push(array[i].Game_ID);
                    }
                }
            }
            if (result2.length === 0) {
                res.json({
                    result: true,
                    data: [],
                });
                return;
            }
            const list_id = result2.join(",");
            const query = `SELECT "Game_ID", "Genre", "Price", "Rate", "Image_path", "Name", "Subtitle" FROM products WHERE "Game_ID" IN (${list_id}) ALLOW FILTERING;`;
            const d2 = await client.execute(query, { prepare: true });
            const result3 = d2.rows;

            res.json({
                result: true,
                data: result3,
            });
        } catch (error) {
            console.error(error);
            res.json({
                result: false,
                error: error.message,
            });
        }
    });

    app.listen(PORT, () => {
        console.log(`listening on port ${PORT}...`);
    });
}

startServer();
