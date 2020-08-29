const express = require("express");
const mysql = require("mysql");
require("dotenv").config();
const cors = require("cors");

const app = express();
app.use(cors());
const PORT = process.env.PORT || 8080;

// Cloud SQL setup code
////////////////////////
var config = {
  user: process.env.SQL_USER,
  database: process.env.SQL_DATABASE,
  password: process.env.SQL_PASSWORD,
};

if (
  process.env.INSTANCE_CONNECTION_NAME &&
  process.env.NODE_ENV === "production"
) {
  config.socketPath = `/cloudsql/${process.env.INSTANCE_CONNECTION_NAME}`;
} else {
  config.host = process.env.SQL_HOST;
}

var connection = mysql.createConnection(config);

connection.connect();
////////////////////////

app.get("/", (req, res) => res.send("Backend App Engine Instance Running"));

// Returns all data about every intersection in the database
app.get("/intersections", (req, res) => {
  connection.query(
    `CREATE OR REPLACE VIEW joinedIntersections AS
    SELECT location.name AS name, latitude, longitude, num_collisions, north, south, east, west
    FROM (location LEFT JOIN collision ON location.name = collision.name LEFT JOIN traffic_volume ON location.name = traffic_volume.name) 
    ORDER BY name`,
    function (err, result, fields) {
      if (err) throw err;
    }
  );

  connection.query(`SELECT * FROM joinedIntersections`, function (
    err,
    result,
    fields
  ) {
    if (err) throw err;
    res.send(result);
  });
});

// Returns data about two intersections to compare them
app.get(
  "/compareIntersections/:firstIntersection/:secondIntersection",
  (req, res) => {
    const firstIntersection = req.params.firstIntersection;
    const secondIntersection = req.params.secondIntersection;

    connection.query(
      "SELECT name, num_collisions, total_traffic, danger_ratio FROM danger WHERE name = ? OR name = ?",
      [firstIntersection, secondIntersection],
      function (err, result, fields) {
        if (err) throw err;
        res.send(result);
      }
    );
  }
);

/////////////////////////////////////////////////////////////////////////////////
/// Update Collisions Endpoints
/////////////////////////////////////////////////////////////////////////////////

// Adds a new intersection to the intersection table
app.post(
  "/addNewIntersection/:intersectionName/:latitude/:longitude",
  (req, res) => {
    const intersectionName = req.params.intersectionName;
    const latitude = parseFloat(req.params.latitude);
    const longitude = parseFloat(req.params.longitude);

    // If it already exists, do nothing
    connection.query(
      "INSERT IGNORE INTO location(name, latitude, longitude) VALUES (?, ?, ?)",
      [intersectionName, latitude, longitude],
      function (err, result, fields) {
        if (err) throw err;
        res.status(200).send("Intersection Added");
      }
    );
  }
);

// Updates the number of collisions at the specified intersection to the specified amount
app.post("/updateCollisions/:intersectionName/:numCollisions", (req, res) => {
  const intersectionName = req.params.intersectionName;
  const numCollisions = parseInt(req.params.numCollisions, 10);

  // If the intersection doesn't exist in the relation, then add it
  connection.query(
    "INSERT IGNORE INTO collision(name,num_collisions) VALUES (?,?)",
    [intersectionName, numCollisions],
    function (err, result, fields) {
      if (err) throw err;
    }
  );

  connection.query(
    "UPDATE collision SET num_collisions = ? WHERE name = ?",
    [numCollisions, intersectionName],
    function (err, result, fields) {
      if (err) throw err;
      res.status(200).send("Collisions Updated");
    }
  );
});

// Increments the collision data for the specified intersection
app.post("/addCollision/:intersectionName", (req, res) => {
  const intersectionName = req.params.intersectionName;

  // If the intersection doesn't exist in the relation, then add it
  connection.query(
    "INSERT IGNORE INTO collision(name,num_collisions) VALUES (?,0)",
    intersectionName,
    function (err, result, fields) {
      if (err) throw err;
    }
  );

  connection.query(
    "UPDATE collision SET num_collisions = num_collisions + 1 WHERE name = ?",
    intersectionName,
    function (err, result, fields) {
      if (err) throw err;
      res.status(200).send("Collision Added");
    }
  );
});

////////////////////////////////////////////////////////////////////////////////

// Returns all information about a specified intersection
app.get("/intersection/:intersectionName", (req, res) => {
  const intersectionName = req.params.intersectionName;
  connection.query(
    `SELECT * FROM joinedIntersections WHERE name = ?`,
    intersectionName,
    function (err, result, fields) {
      if (err) throw err;
      res.send(result);
    }
  );
});

// Gets the top X most dangerous intersections
app.get("/collisions/:displayNum", (req, res) => {
  connection.query(
    `
    CREATE OR REPLACE VIEW total_traffic_volume AS
    SELECT name, (north + south + east + west) as total_traffic
    FROM traffic_volume;
    `,
    function (err, result, fields) {
      if (err) throw err;
    }
  );

  connection.query(
    `
    CREATE OR REPLACE VIEW total_collision AS
    SELECT location.name, COALESCE(num_collisions, 0) as num_collisions
    FROM (location LEFT OUTER JOIN collision ON location.name=collision.name);
    `,
    function (err, result, fields) {
      if (err) throw err;
    }
  );

  connection.query(
    `
    CREATE OR REPLACE VIEW danger AS
    SELECT collision_traffic.name, collision_traffic.num_collisions/collision_traffic.total_traffic as danger_ratio, collision_traffic.num_collisions, collision_traffic.total_traffic
    FROM (
        (SELECT total_collision.name, num_collisions, total_traffic
        FROM (total_collision JOIN total_traffic_volume ON total_collision.name=total_traffic_volume.name)
        WHERE total_traffic IS NOT NULL
        )
        AS collision_traffic
    )`,
    function (err, result, fields) {
      if (err) throw err;
    }
  );

  const displayNum = parseInt(req.params.displayNum, 10);
  console.log(displayNum);

  connection.query(
    "SELECT name, num_collisions, total_traffic, danger_ratio FROM danger ORDER BY danger_ratio DESC LIMIT " +
      connection.escape(displayNum) +
      "",
    function (err, result, fields) {
      if (err) throw err;
      res.send(result);
    }
  );
});

// Gets all intersections within a given Latitude and Longitude Range
app.get(
  "/intersectionsWithinRange/:minLatitude/:maxLatitude/:minLongitude/:maxLongitude",
  (req, res) => {
    const minLatitude = parseFloat(req.params.minLatitude);
    const maxLatitude = parseFloat(req.params.maxLatitude);
    const minLongitude = parseFloat(req.params.minLongitude);
    const maxLongitude = parseFloat(req.params.maxLongitude);

    connection.query(
      "SELECT * FROM joinedIntersections WHERE latitude >= ? AND latitude <= ? AND longitude >= ? AND longitude <= ?",
      [minLatitude, maxLatitude, minLongitude, maxLongitude],
      function (err, result, fields) {
        if (err) throw err;
        res.send(result);
      }
    );
  }
);

// Gets all intersections which intersect with the given street
app.get("/intersectionsWithStreet/:streetName", (req, res) => {
  const streetName = "%" + req.params.streetName + "%";

  connection.query(
    "SELECT * FROM joinedIntersections WHERE name LIKE ?",
    streetName,
    function (err, result, fields) {
      if (err) throw err;
      res.send(result);
    }
  );
});

app.listen(PORT, () => {
  console.log(`App listening on port ${PORT}`);
});
