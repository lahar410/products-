
// validateGrossWeights().catch(console.error);
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');
const { parse } = require('json2csv');

// MongoDB connection string
// const MONGO_URI = 'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true';
const MONGO_URI ='mongodb+srv://CRM-Database-ReadOnly:1cS8EhFkOzKwlVuI@cluster0.okk1w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0&readPreference=secondaryPreferred'

const productSchema = new mongoose.Schema({}, { strict: false });
const Product = mongoose.model('Product', productSchema, 'products');

async function validateGrossWeights() {
  await mongoose.connect(MONGO_URI, {
    dbName: 'CRM-Database',
  });

  const allProducts = await Product.find({ parentSku: { $exists: true, $ne: null } });

  const correct = [];
  const incorrect = [];

  for (const skuDoc of allProducts) {
    const {
      sku,
      parentSku,
      grossWeight: skuGrossWeight,
      itemQty,
      displayName: skuDisplayName,
    } = skuDoc;

    if (!sku || !parentSku || itemQty == null) continue;

    const parent = await Product.findOne({ sku: parentSku });
    if (!parent) continue;

    const parentGrossWeight = parent.grossWeight || 0;
    const expectedGrossWeight = itemQty * parentGrossWeight;

    const isCorrect =
      Math.abs((skuGrossWeight || 0) - expectedGrossWeight) < 0.001;

    const entry = {
      skuDisplayName,
      parentDisplayName: parent.displayName || '',
      sku,
      parentSku,
      itemQty,
      skuGrossWeight,
      parentGrossWeight,
      ...(isCorrect ? {} : { expectedGrossWeight }),
    };

    if (isCorrect) {
      correct.push(entry);
    } else {
      incorrect.push(entry);
    }
  }

  const fields = [
    'skuDisplayName',
    'parentDisplayName',
    'sku',
    'parentSku',
    'itemQty',
    'skuGrossWeight',
    'parentGrossWeight',
    'expectedGrossWeight',
  ];

  // Write correct entries if available
  if (correct.length > 0) {
    const correctCSV = parse(correct, { fields });
    fs.writeFileSync(path.join(__dirname, 'correct_grossweightsMAIN.csv'), correctCSV);
  } else {
    console.log('ℹ️ No correct entries found.');
  }

  // Write incorrect entries if available
  if (incorrect.length > 0) {
    const incorrectCSV = parse(incorrect, { fields });
    fs.writeFileSync(path.join(__dirname, 'incorrect_grossweightsMAIN.csv'), incorrectCSV);
  } else {
    console.log('ℹ️ No incorrect entries found.');
  }

  console.log(`✅ Done. Correct: ${correct.length}, Incorrect: ${incorrect.length}`);
  await mongoose.disconnect();
}

validateGrossWeights().catch(console.error);
