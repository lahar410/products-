const mongoose = require('mongoose');
const fs = require('fs');
const { Parser } = require('json2csv');

// Replace with your actual MongoDB URI
const MONGODB_URI = '';

const productSchema = new mongoose.Schema({
  sku: String,
  parentSku: String,
  displayName: String,
  itemQty: Number,
  grossWeight: Number,
}, { collection: 'products' });

const Product = mongoose.model('Product', productSchema);

function extractQtyFromDisplayName(displayName) {
  if (!displayName) return null;
  const match = displayName.match(/ x (\d+)$/i);
  return match ? parseInt(match[1]) : 1;
}

async function validateProducts() {
  await mongoose.connect(MONGODB_URI);

  const products = await Product.find({});
  const incorrectProducts = [];

  for (const product of products) {
    const {
      sku,
      parentSku,
      displayName,
      itemQty,
      grossWeight,
    } = product;

    const extractedQty = extractQtyFromDisplayName(displayName);

    const isQtyCorrect = extractedQty === itemQty;
    const isParentSkuCorrect = (extractedQty === 1 && sku === parentSku)
      || (extractedQty > 1 && sku !== parentSku);

    if (!isQtyCorrect || !isParentSkuCorrect) {
      incorrectProducts.push({
        sku,
        parentSku,
        displayName,
        itemQty,
        expectedItemQty: extractedQty,
        grossWeight,
        correctParentSku: extractedQty === 1 ? sku : 'DIFFERENT FROM SKU',
        itemQtyMatch: isQtyCorrect,
        parentSkuMatch: isParentSkuCorrect,
      });
    }
  }

  if (incorrectProducts.length) {
    const parser = new Parser();
    const csv = parser.parse(incorrectProducts);
    fs.writeFileSync('incorrect_products.csv', csv);
    console.log('CSV created: incorrect_products.csv');
  } else {
    console.log('✅ All products are correct.');
  }

  await mongoose.disconnect();
}

validateProducts().catch(err => {
  console.error('Error:', err);
  mongoose.disconnect();
});





