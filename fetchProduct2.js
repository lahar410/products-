
const mongoose = require('mongoose');
const fs = require('fs');
const csv = require('csv-parser');
const { format } = require('@fast-csv/format');
const { log } = require('console');

// MongoDB Connection
mongoose.connect(
  'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true',
  {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    dbName: 'CRM-Database',
    serverSelectionTimeoutMS: 5000,
  },
  console.log('✅ MongoDB Connected')
);

// // Define Schemas
// const Category = mongoose.model('Category', new mongoose.Schema({ name: String }));
// const Sub_Category = mongoose.model('subCategories', new mongoose.Schema({ name: String }));
// const Bulk = mongoose.model('Bulk', new mongoose.Schema({ sku: String }));

// const Product = mongoose.model('Product', new mongoose.Schema({
//   sku: String,
//   parentSku: String,
//   variant_name: String,
//   displayName: String,
//   grossWeight: Number,
//   netWeight: Number,
//   itemQty: Number,
//   category: { type: mongoose.Schema.Types.ObjectId, ref: 'Category' },
//   sub_category: { type: mongoose.Schema.Types.ObjectId, ref: 'subCategories' },
//   bulk: { type: mongoose.Schema.Types.ObjectId, ref: 'Bulk' },
// }));

// // Function to get objectId -> name mapping
// async function getLookupMaps() {
//   const [categories, sub_categories, bulks] = await Promise.all([
//     Category.find({}),
//     Sub_Category.find({}),
//     Bulk.find({})
//   ]);

//   const categoryMap = new Map(categories.map(c => [c._id.toString(), c.name]));
//   const subCategoryMap = new Map(sub_categories.map(sc => [sc._id.toString(), sc.name]));
//   const bulkMap = new Map(bulks.map(b => [b._id.toString(), b.sku]));

//   console.log("categoryMap",categoryMap)
//   console.log("subCategoryMap",subCategoryMap)
//   console.log("bulkMap",bulkMap)

//   return { categoryMap, subCategoryMap, bulkMap };
// }

// async function processCSV(inputFilePath, outputFilePath, incorrectOutputFilePath) {
//   const csvRows = [];
//   const results = [];
//   const incorrectRows = [];

//   fs.createReadStream(inputFilePath)
//     .pipe(csv({ separator: ',' }))
//     .on('data', (data) => csvRows.push(data))
//     .on('end', async () => {
//       const { categoryMap, subCategoryMap, bulkMap } = await getLookupMaps();

//       for (const row of csvRows) {
//         const product = await Product.findOne({ sku: row.sku })
//           .populate('category')
//           .populate('sub_category')
//           .populate('bulk');

//         if (!product) {
//           const errorRow = { ...row, error: 'SKU not found in DB' };
//           results.push(errorRow);
//           incorrectRows.push({ ...errorRow, mismatch_fields: 'sku' });
//           continue;
//         }

//         const mongoData = {
//           sku: product.sku,
//           parentSku: product.parentSku || '',
//           displayName: product.displayName || '',
//           variant_name: product.variant_name || '',
//           grossWeight: product.grossWeight?.toString() || '',
//           netWeight: product.netWeight?.toString() || '',
//           itemQty: product.itemQty?.toString() || '',
//           category: categoryMap.get(product.category?._id.toString()) || '',
//           sub_category: subCategoryMap.get(product.sub_category?._id.toString()) || '',
//           bulk: bulkMap.get(product.bulk?._id.toString()) || '',
//         };

//         const rowResult = {
//           ...row,
//           mongodb_displayName: mongoData.displayName,
//           isCorrect_displayName: row.displayName?.trim() === mongoData.displayName,

//           mongodb_variant_name: mongoData.variant_name,
//           isCorrect_variant_name: row.variant_name?.trim() === mongoData.variant_name,

//           mongodb_grossWeight: mongoData.grossWeight,
//           isCorrect_grossWeight: row.grossWeight?.trim() === mongoData.grossWeight,

//           mongodb_netWeight: mongoData.netWeight,
//           isCorrect_netWeight: row.netWeight?.trim() === mongoData.netWeight,

//           mongodb_itemQty: mongoData.itemQty,
//           isCorrect_itemQty: row.itemQty?.trim() === mongoData.itemQty,

//           mongodb_category: mongoData.category,
//           isCorrect_category: row.category?.trim() === mongoData.category,

//           mongodb_sub_category: mongoData.sub_category,
//           isCorrect_sub_category: row.sub_category?.trim() === mongoData.sub_category,

//           mongodb_bulkSku: mongoData.bulk,
//           isCorrect_bulkSku: row.bulkSku?.trim() === mongoData.bulk,
//         };

//         // Collect incorrect fields
//         const mismatchFields = [];
//         Object.keys(rowResult).forEach(key => {
//           if (key.startsWith('isCorrect_') && rowResult[key] === false) {
//             mismatchFields.push(key.replace('isCorrect_', ''));
//           }
//         });

//         // Add mismatch field info
//         if (mismatchFields.length > 0) {
//           incorrectRows.push({ ...rowResult, mismatch_fields: mismatchFields.join(',') });
//         }

//         results.push(rowResult);
//       }

//       // Write all results to main output
//       const allOut = fs.createWriteStream(outputFilePath);
//       const csvAll = format({ headers: true });
//       csvAll.pipe(allOut);
//       results.forEach(row => csvAll.write(row));
//       csvAll.end();

//       // Write only incorrect rows to separate file
//       const incorrectOut = fs.createWriteStream(incorrectOutputFilePath);
//       const csvIncorrect = format({ headers: true });
//       csvIncorrect.pipe(incorrectOut);
//       incorrectRows.forEach(row => csvIncorrect.write(row));
//       csvIncorrect.end();

//       console.log('✅ Processing completed.');
//       console.log('✔️ All products written to:', outputFilePath);
//       console.log('❌ Incorrect products written to:', incorrectOutputFilePath);
//       mongoose.disconnect();
//     });
// }

// // Run it
// processCSV('./products.csv', './outputallproduct.csv', './incorrectProduct.csv');


// const mongoose = require('mongoose');
// const fs = require('fs');
// const csv = require('csv-parser');
// const { format } = require('@fast-csv/format');

// // MongoDB Connection
// mongoose.connect(
//  'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true',
//   {
//     useNewUrlParser: true,
//     useUnifiedTopology: true,
//     dbName: 'CRM-Database',
//     serverSelectionTimeoutMS: 5000,
//   },
//   () => console.log('✅ MongoDB Connected')
// );

// Define Schemas
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

        // Write both CSVs
        
//         for (const row of csvRows) {
//   const product = productMap.get(row.sku?.trim());
//   if (!product) {
//     const errorRow = {
//       sku: row.sku,
//       error: 'SKU not found in DB',
//       mismatch_fields: 'sku'
//     };
//     allResults.push({ ...row, error: errorRow.error, mismatch_fields: errorRow.mismatch_fields });
//     incorrectResults.push(errorRow);
//     continue;
//   }

//   const dbValues = {
//     displayName: product.displayName || '',
//     variant_name: product.variant_name || '',
//     grossWeight: product.grossWeight?.toString() || '',
//     netWeight: product.netWeight?.toString() || '',
//     itemQty: product.itemQty?.toString() || '',
//     category: categoryMap[product.category?._id.toString()] || '',
//     sub_category: subCategoryMap[product.sub_category?._id.toString()] || '',
//     bulk: bulkMap[product.bulk?._id.toString()] || '',
//   };

//   const rowResult = { ...row };
//   const incorrectRow = { sku: row.sku };
//   const mismatches = [];

//   for (const key in dbValues) {
//     const originalKey = key === 'bulkSku' ? 'bulkSku' : key;
//     const csvValue = row[originalKey]?.trim() || '';
//     const dbValue = dbValues[key];
//     rowResult[`mongodb_${key}`] = dbValue;
//     const isCorrect = csvValue === dbValue;
//     rowResult[`isCorrect_${key}`] = isCorrect;

//     if (!isCorrect) {
//       incorrectRow[originalKey] = csvValue;
//       incorrectRow[`mongodb_${originalKey}`] = dbValue;
//       mismatches.push(originalKey);
//     }
//   }

//   if (mismatches.length > 0) {
//     incorrectRow.mismatch_fields = mismatches.join(',');
//     incorrectResults.push(incorrectRow);
//   }

//   allResults.push(rowResult);
// }

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
processCSV('./bookingsheetProduct.csv', './outputallproduct8.csv', './incorrectProduct8.csv');
