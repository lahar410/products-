const mongoose = require('mongoose');
const fs = require('fs');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// MongoDB Connection
mongoose.connect('mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  dbName: 'CRM-Database',

});

// Define Product Schema
const productSchema = new mongoose.Schema({
  sku: String,
  parentSku: String,
  itemQty: Number,
});

// Define Combo Schema
const comboSchema = new mongoose.Schema({
    comboSku:String,
  products: [
    {
      product: { type: mongoose.Schema.Types.ObjectId, ref: 'Product' },
      sku: String,
    },
  ],
});

const Product = mongoose.model('Product', productSchema);
const Combo = mongoose.model('Combo', comboSchema);

// CSV Writer setup
const csvWriter = createCsvWriter({
  path: 'combo_products_output33.csv',
  header: [
    { id: 'comboId', title: 'ComboId' },
    {id:'comboSku',title:'comboSku'},
    { id: 'sku', title: 'SKU' },
    { id: 'parentSku', title: 'ParentSKU' },
    { id: 'itemQty', title: 'ItemQty' },
  ],
});

async function generateCSV() {
  try {
    const combos = await Combo.find({}).lean();

    const results = [];

    for (const combo of combos) {
      const comboId = combo._id.toString();
      const comboSku = combo.comboSku;
      for (const item of combo.products) {
        const productDoc = await Product.findById(item.product).lean();
        if (productDoc) {
          results.push({
            comboId,
            comboSku,
            sku: productDoc.sku || item.sku,
            parentSku: productDoc.parentSku ,
            itemQty: productDoc.itemQty ,
          });
        }
      }
    }

    await csvWriter.writeRecords(results);
    console.log('✅ CSV file written successfully as combo_products_output.csv');
    mongoose.disconnect();
  } catch (error) {
    console.error('❌ Error generating CSV:', error);
    mongoose.disconnect();
  }
}

generateCSV();
