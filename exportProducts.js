const mongoose = require("mongoose");
const fs = require("fs");
const { Parser } = require("json2csv");

// MongoDB connection
mongoose.connect('mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true'
, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
dbName: 'CRM-Database',

});

// Define product schema
const productSchema = new mongoose.Schema({
  displayName: String,
  sku: String,
  parentSku: String,
  itemQty: Number,
  grossWeight: Number,
  is_parent: Boolean,
});

const Product = mongoose.model("Product", productSchema);

async function exportCSV() {
  try {
    const allProducts = await Product.find({}).lean();

    const parentProducts = [];
    const childProducts = [];

    for (const product of allProducts) {
      if (
        product.sku === product.parentSku &&
        product.is_parent === true
      ) {
        parentProducts.push(product);
      } else {
        childProducts.push(product);
      }
    }

    // Fields to export
    const fields = [
      "displayName",
      "sku",
      "parentSku",
      "itemQty",
      "grossWeight",
      "is_parent",
    ];

    const parser = new Parser({ fields });

    // Convert to CSV
    const parentCsv = parser.parse(parentProducts);
    const childCsv = parser.parse(childProducts);

    // Write CSV files
    fs.writeFileSync("parent_products.csv", parentCsv);
    fs.writeFileSync("child_products.csv", childCsv);

    console.log("✅ CSV files created: parent_products.csv, child_products.csv");

    mongoose.disconnect();
  } catch (error) {
    console.error("❌ Error:", error);
    mongoose.disconnect();
  }
}

exportCSV();
