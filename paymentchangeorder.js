const fs = require("fs");
const csv = require("csv-parser");
const { format } = require("@fast-csv/format");

// Paths
const orderCSV = "one.csv";       // has order_name and payment_status
const paymentCSV = "paymentorders.csv";   // has order_id and payment_mode
const outputCSV = "merged.csv";

// Read CSV into memory
function readCSV(filePath, keyField) {
  return new Promise((resolve) => {
    const results = {};
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (data) => {
        results[data[keyField]] = data;
      })
      .on("end", () => {
        resolve(results);
      });
  });
}

async function mergeCSVs() {
  const orders = await readCSV(orderCSV, "order_name");
  const payments = await readCSV(paymentCSV, "order_id");

  const merged = [];

  for (const orderId in orders) {
    const order = orders[orderId];
    const payment = payments[orderId] || {};

    merged.push({
      order_name: order.order_name,
      payment_status: order.payment_status,
      payment_mode: payment.payment_mode || "N/A",
    });
  }

  const ws = fs.createWriteStream(outputCSV);
  const csvStream = format({ headers: true });
  csvStream.pipe(ws);

  merged.forEach((row) => csvStream.write(row));
  csvStream.end(() => console.log("✅ Merged CSV written to:", outputCSV));
}

mergeCSVs();
