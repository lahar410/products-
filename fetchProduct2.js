
const mongoose = require('mongoose');
const fs = require('fs');
const csv = require('csv-parser');
const { format } = require('@fast-csv/format');
const { log } = require('console');

// MongoDB Connection
// MongoDB Connection
mongoose.connect('mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  dbName: 'CRM-Database',
});

const Category = mongoose.model('Category', new mongoose.Schema({ name: String }));
const subCategories = mongoose.model('subCategories', new mongoose.Schema({ name: String }));
const Bulk = mongoose.model('Bulk', new mongoose.Schema({ sku: String }));

const Product = mongoose.model('Product', new mongoose.Schema({
  sku: String,
  parentSku: String,
  variant_name: String,
  displayName: String,
  grossWeight: Number,
  netWeight: Number,
  itemQty: Number,
  category: { type: mongoose.Schema.Types.ObjectId, ref: 'Category' },
  sub_category: { type: mongoose.Schema.Types.ObjectId, ref: 'subCategories' },
  bulk: { type: mongoose.Schema.Types.ObjectId, ref: 'Bulk' },
}));

async function getLookupMaps() {
  const [categories, sub_categories, bulks] = await Promise.all([
    Category.find(),
    subCategories.find(),
    Bulk.find()
  ]);
    

  return {
    categoryMap: Object.fromEntries(categories.map(c => [c._id.toString(), c.name])),
    subCategoryMap: Object.fromEntries(sub_categories.map(sc => [sc._id.toString(), sc.name])),
    bulkMap: Object.fromEntries(bulks.map(b => [b._id.toString(), b.sku]))
  };
  
}

function writeCSV(filePath, data) {
  const stream = fs.createWriteStream(filePath);
  const csvStream = format({ headers: true });
  csvStream.pipe(stream);
  data.forEach(row => csvStream.write(row));
  csvStream.end();
}

async function processCSV(inputFilePath, outputFilePath, incorrectOutputFilePath) {
  const csvRows = [];

  fs.createReadStream(inputFilePath)
    .pipe(csv())
    .on('data', row => csvRows.push(row))
    .on('end', async () => {
      try {
        const { categoryMap, subCategoryMap, bulkMap } = await getLookupMaps();
        const skuList = csvRows.map(row => row.sku.trim());
        const products = await Product.find({ sku: { $in: skuList } })
          .populate('category')
          .populate('sub_category')
          .populate('bulk');

        const productMap = new Map(products.map(p => [p.sku, p]));

        const allResults = [];
        const incorrectResults = [];

        for (const row of csvRows) {
          const product = productMap.get(row.sku?.trim());
          if (!product) {
            const errorRow = { ...row, error: 'SKU not found in DB', mismatch_fields: 'sku' };
            allResults.push(errorRow);
            incorrectResults.push(errorRow);
            continue;
          }

          const dbValues = {

            displayName: product.displayName || '',
            parentSku: product.parentSku || '',
            variant_name: product.variant_name || '',
            grossWeight: product.grossWeight?.toString() || '',
            netWeight: product.netWeight?.toString() || '',
            itemQty: product.itemQty?.toString() || '',
            category: categoryMap[product.category?._id.toString()] || '',
            sub_category: subCategoryMap[product.sub_category?._id.toString()] || '',
            bulk: bulkMap[product.bulk?._id.toString()] || '',
          };

          const rowResult = { ...row };
          const mismatches = [];

          for (const key in dbValues) {
            const originalKey = key === 'bulkSku' ? 'bulkSku' : key;
            const csvValue = row[originalKey]?.trim() || '';
            const dbValue = dbValues[key];
            rowResult[`mongodb_${key}`] = dbValue;
            const isCorrect = csvValue === dbValue;
            rowResult[`isCorrect_${key}`] = isCorrect;
            if (!isCorrect) mismatches.push(key);
          }

          if (mismatches.length > 0) {
            rowResult.mismatch_fields = mismatches.join(',');
            incorrectResults.push(rowResult);
          }

          allResults.push(rowResult);
        }



        writeCSV(outputFilePath, allResults);
        writeCSV(incorrectOutputFilePath, incorrectResults);

        console.log('✅ CSV processing complete.');
        console.log(`✔️ All rows written to ${outputFilePath}`);
        console.log(`❌ Incorrect rows written to ${incorrectOutputFilePath}`);
      } catch (error) {
        console.error('❌ Error during CSV processing:', error);
      } finally {
        mongoose.disconnect();
      }
    });
}

// Start processing
processCSV('./finalproduct.csv', './outputallproduct15.csv', './incorrectProduct15.csv');
