// const mongoose = require('mongoose');
// const fs = require('fs');
// const csv = require('csv-parser');
// const { format } = require('@fast-csv/format');

// // MongoDB Connection
// mongoose.connect('mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true'
// , {
//   useNewUrlParser: true,
//   useUnifiedTopology: true,
//   dbName: "CRM-Database",
//     serverSelectionTimeoutMS: 5000, // Set timeout to 5s

//   },
//   console.log("connection connect "),
// );

// // Define Schemas
// const Category = mongoose.model('Category', new mongoose.Schema({ name: String }));
// const subCategories = mongoose.model('subCategories', new mongoose.Schema({ name: String }));
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

// console.log("schema ready for category , subcatgeoy , bulk , products ")

// // Function to get objectId -> name mapping
// async function getLookupMaps() {
//   const [categories, sub_categories, bulks] = await Promise.all([
//     Category.find({}),
//     subCategories.find({}),
//     Bulk.find({})
//   ]);

//   const categoryMap = new Map(categories.map(c => [c._id.toString(), c.name]));
//   const subCategoryMap = new Map(sub_categories.map(sc => [sc._id.toString(), sc.name]));
//   const bulkMap = new Map(bulks.map(b => [b._id.toString(), b.sku]));

//   console.log("categoryMap===",categoryMap)

//   console.log("subCategoryMap===========",subCategoryMap)
//   console.log("bulkMap=============",bulkMap)
//   return { categoryMap, subCategoryMap, bulkMap };
// }


// console.log("loopup for  ready for category , subcatgeoy , bulk , products ")

// // Main logic
// async function processCSV(inputFilePath, outputFilePath) {
//   const csvRows = [];
//   const results = [];

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
// console.log(" , products   ")

//         if (!product) {
//           results.push({
//             ...row,
//             error: 'SKU not found in DB',
//           });
//           continue;
//         }

//         const mongoData = {
//   sku: product.sku,
//   parentSku: product.parentSku || '',
//   displayName: product.displayName || '',
//   variant_name: product.variant_name || '',
//   grossWeight: product.grossWeight?.toString() || '',
//   netWeight: product.netWeight?.toString() || '',
//   itemQty: product.itemQty?.toString() || '',
//   category: categoryMap.get(product.category?._id.toString()) || '',
//   sub_category: subCategoryMap.get(product.sub_category?._id.toString()) || '',
//   bulk: bulkMap.get(product.bulk?._id.toString()) || '',
// };

// results.push({
//   ...row,
//   actual_displayName: mongoData.displayName,
//   isCorrect_displayName: row.displayName?.trim() === mongoData.displayName?.trim(),

//   actual_variant_name: mongoData.variant_name,
//   isCorrect_variant_name: row.variant_name?.trim() === mongoData.variant_name?.trim(),

//   actual_grossWeight: mongoData.grossWeight,
//   isCorrect_grossWeight: row.grossWeight?.trim() === mongoData.grossWeight,

//   actual_netWeight: mongoData.netWeight,
//   isCorrect_netWeight: row.netWeight?.trim() === mongoData.netWeight,

//   actual_itemQty: mongoData.itemQty,
//   isCorrect_itemQty: row.itemQty?.trim() === mongoData.itemQty,

//   actual_category: mongoData.category,
//   isCorrect_category: row.category?.trim() === mongoData.category,

//   actual_sub_category: mongoData.sub_category,
//   isCorrect_sub_category: row.sub_category?.trim() === mongoData.sub_category,

//   actual_bulkSku: mongoData.bulk,
//   isCorrect_bulkSku: row.bulkSku?.trim() === mongoData.bulk,
// });

//         // const mongoData = {
//         //   sku: product.sku,
//         //   parentSku: product.parentSku || '',
//         //   displayName: product.displayName || '',
//         //   variant_name: product.variant_name || '',
//         //   grossWeight: product.grossWeight || '',
//         //   netWeight: product.netWeight || '',
//         //   itemQty: product.itemQty || '',
//         //   category: categoryMap.get(product.category?._id.toString()) || '',
//         //   sub_category: subCategoryMap.get(product.sub_category?._id.toString()) || '',
//         //   bulk: bulkMap.get(product.bulk?._id.toString()) || '',
//         // };

//         // results.push({
//         //   ...row,
//         //   actual_category: mongoData.category,
//         //   actual_sub_category: mongoData.sub_category,
//         //   actual_bulkSku: mongoData.bulk,
//         //   actual_displayName: mongoData.displayName,
//         //   actual_grossWeight: mongoData.grossWeight,
//         //   actual_netWeight: mongoData.netWeight,
//         //   actual_itemQty: mongoData.itemQty,
//         //   actual_variant_name: mongoData.variant_name,
//         // });
//       }

//       // Write to output CSV
//       const ws = fs.createWriteStream(outputFilePath);
//       const csvStream = format({ headers: true });
//       csvStream.pipe(ws);
//       results.forEach(row => csvStream.write(row));
//       csvStream.end();

//       console.log('✅ Processing completed. Output written to:', outputFilePath);
//       mongoose.disconnect();
//     });
// }

// // Run
// processCSV('./products.csv', './outputProduct23.csv');







const mongoose = require('mongoose');
const fs = require('fs');
const csv = require('csv-parser');
const { format } = require('@fast-csv/format');

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

// Lookup maps for ObjectId -> name
async function getLookupMaps() {
  const [categories, sub_categories, bulks] = await Promise.all([
    Category.find({}),
    subCategories.find({}),
    Bulk.find({}),
  ]);

  const categoryMap = new Map(categories.map(c => [c._id.toString(), c.name]));
  const subCategoryMap = new Map(sub_categories.map(sc => [sc._id.toString(), sc.name]));
  const bulkMap = new Map(bulks.map(b => [b._id.toString(), b.sku]));

  console.log("categoryMap",categoryMap)
  console.log("subCategoryMap",subCategoryMap)
  console.log("bulkMap",bulkMap)
  return { categoryMap, subCategoryMap, bulkMap };
}

// Main logic
async function processCSV(inputFilePath, outputFilePath) {
  const csvRows = [];

  // Step 1: Read CSV into memory
  await new Promise((resolve, reject) => {
    fs.createReadStream(inputFilePath)
      .pipe(csv({ separator: ',' }))
      .on('data', (data) => csvRows.push(data))
      .on('end', resolve)
      .on('error', reject);
  });

  console.log(`📦 Loaded ${csvRows.length} rows from CSV`);

  const { categoryMap, subCategoryMap, bulkMap } = await getLookupMaps();
  const results = [];

  // Step 2: Process in batches of 100
  for (let i = 0; i < csvRows.length; i += 100) {
    const batch = csvRows.slice(i, i + 100);
    const skus = batch.map(row => row.sku);

    const products = await Product.find({ sku: { $in: skus } })
      .populate('category')
      .populate('sub_category')
      .populate('bulk');

    const productMap = new Map(products.map(p => [p.sku, p]));

    for (const row of batch) {
      const product = productMap.get(row.sku);

      if (!product) {
        results.push({ ...row, error: 'SKU not found in DB' });
        continue;
      }

      const mongoData = {
        sku: product.sku,
        parentSku: product.parentSku || '',
        displayName: product.displayName || '',
        variant_name: product.variant_name || '',
        grossWeight: product.grossWeight?.toString() || '',
        netWeight: product.netWeight?.toString() || '',
        itemQty: product.itemQty?.toString() || '',
        category: categoryMap.get(product.category?._id.toString()) || '',
        sub_category: subCategoryMap.get(product.sub_category?._id.toString()) || '',
        bulk: bulkMap.get(product.bulk?._id.toString()) || '',
      };

      results.push({
        ...row,
        actual_displayName: mongoData.displayName,
        isCorrect_displayName: row.displayName?.trim() === mongoData.displayName?.trim(),

        actual_variant_name: mongoData.variant_name,
        isCorrect_variant_name: row.variant_name?.trim() === mongoData.variant_name?.trim(),

        actual_grossWeight: mongoData.grossWeight,
        isCorrect_grossWeight: row.grossWeight?.trim() === mongoData.grossWeight,

        actual_netWeight: mongoData.netWeight,
        isCorrect_netWeight: row.netWeight?.trim() === mongoData.netWeight,

        actual_itemQty: mongoData.itemQty,
        isCorrect_itemQty: row.itemQty?.trim() === mongoData.itemQty,

        actual_category: mongoData.category,
        isCorrect_category: row.category?.trim() === mongoData.category,

        actual_sub_category: mongoData.sub_category,
        isCorrect_sub_category: row.sub_category?.trim() === mongoData.sub_category,

        actual_bulkSku: mongoData.bulk,
        isCorrect_bulkSku: row.bulkSku?.trim() === mongoData.bulk,
      });
    }
  }

  // Step 3: Write output CSV
  const ws = fs.createWriteStream(outputFilePath);
  const csvStream = format({ headers: true });
  csvStream.pipe(ws);
  results.forEach(row => csvStream.write(row));
  csvStream.end();

  console.log(`✅ Done. Processed ${results.length} rows. Output written to ${outputFilePath}`);
  mongoose.disconnect();
}

// Run
processCSV('./products.csv', './outputProduct24.csv');
