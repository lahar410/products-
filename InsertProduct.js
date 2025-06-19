// const mongoose = require('mongoose');
// const fs = require('fs');
// const csv = require('csv-parser');

// // ====== MongoDB Connection ======
// mongoose.connect(
//   'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true',
//   {
//     useNewUrlParser: true,
//     useUnifiedTopology: true,
//     dbName: 'CRM-Database',
//   }
// ).then(() => console.log('✅ MongoDB connected'))
//  .catch(err => console.error('❌ MongoDB connection error:', err));

// // ====== Mongoose Schemas ======
// const bulkSchema = new mongoose.Schema({
//   bulkSku: { type: String },
// });

// const categorySchema = new mongoose.Schema({
//   name: { type: String },
// });

// const subCategorySchema = new mongoose.Schema({
//   name: { type: String },
// });

// const productSchema = new mongoose.Schema({
//   sku: { type: String, required: true, unique: true },
//   parentSku: { type: String },
//   bulk: { type: mongoose.Schema.Types.ObjectId, ref: 'Bulk' },
//   variant_name: { type: String },
//   grossWeight: { type: Number },
//   netWeight: { type: Number },
//   itemQty: { type: Number },
//   displayName: { type: String },
//   category: { type: mongoose.Schema.Types.ObjectId, ref: 'Category' },
//   sub_category: { type: mongoose.Schema.Types.ObjectId, ref: 'SubCategory' },
// }, { timestamps: true });

// // ====== Mongoose Models ======
// const Bulk = mongoose.model('Bulk', bulkSchema);
// const Category = mongoose.model('Category', categorySchema);
// const SubCategory = mongoose.model('SubCategory', subCategorySchema);
// const Product = mongoose.model('Product', productSchema);

// // ====== Upsert Function ======
// const insertOrUpdateProducts = async (productList) => {
//   for (const data of productList) {
//     try {
//       const {
//         sku,
//         parentSku,
//         bulk,
//         variant_name,
//         grossWeight,
//         netWeight,
//         itemQty,
//         displayName,
//         category,
//         sub_category,
//       } = data;

//       const bulkDoc = await Bulk.findOne({ bulkSku: bulk.trim() });
//       const categoryDoc = await Category.findOne({ name: category.trim() });
//       const subCategoryDoc = await SubCategory.findOne({ name: sub_category.trim() });

//       if (!bulkDoc || !categoryDoc || !subCategoryDoc) {
//         console.warn(`⚠️ Missing reference for SKU: ${sku}`);
//         continue;
//       }

//       const updateData = {
//         parentSku,
//         variant_name,
//         grossWeight: parseFloat(grossWeight),
//         netWeight: parseFloat(netWeight),
//         itemQty: parseInt(itemQty),
//         displayName,
//         bulk: bulkDoc._id,
//         category: categoryDoc._id,
//         sub_category: subCategoryDoc._id,
//       };

//       await Product.findOneAndUpdate(
//         { sku: sku.trim() },
//         { $set: updateData },
//         { upsert: true, new: true }
//       );

//       console.log(`✔️ Processed SKU: ${sku}`);
//     } catch (err) {
//       console.error(`❌ Error processing SKU: ${data.sku}`, err);
//     }
//   }

//   console.log('🎉 Done processing all products.');
//   mongoose.disconnect();
// };

// // ====== Load CSV ======
// const productList = [];

// fs.createReadStream('InsertProduct.csv')
//   .pipe(csv())
//   .on('data', (row) => {
//     productList.push({
//       sku: row.sku,
//       parentSku: row.parentSku,
//       bulk: row.bulk,
//       variant_name: row.variant_name,
//       grossWeight: row.grossWeight,
//       netWeight: row.netWeight,
//       itemQty: row.itemQty,
//       displayName: row.displayName,
//       category: row.category,
//       sub_category: row.sub_category,
//     });
//   })
//   .on('end', () => {
//     console.log(`📦 Loaded ${productList.length} products from CSV`);
//     insertOrUpdateProducts(productList);
//   });




const mongoose = require('mongoose');
const fs = require('fs');
const csv = require('csv-parser');
const { format } = require('@fast-csv/format');

// ====== MongoDB Connection ======
mongoose.connect(
  'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true',
  {
    // useNewUrlParser: true,
    // useUnifiedTopology: true,
    dbName: 'CRM-Database',
  }
).then(() => console.log('✅ MongoDB connected'))
  .catch(err => {
    console.error('❌ MongoDB connection error:', err);
    process.exit(1);
  });

// ====== Mongoose Schemas ======
const bulkSchema = new mongoose.Schema({ sku: String });
const categorySchema = new mongoose.Schema({ name: String });
const subCategorySchema = new mongoose.Schema({ name: String });
const productSchema = new mongoose.Schema({
  sku: { type: String, required: true, unique: true },
  parentSku: String,
  bulk: { type: mongoose.Schema.Types.ObjectId, ref: 'Bulk' },
  variant_name: String,
  grossWeight: Number,
  netWeight: Number,
  itemQty: Number,
  displayName: String,
  category: { type: mongoose.Schema.Types.ObjectId, ref: 'Category' },
  sub_category: { type: mongoose.Schema.Types.ObjectId, ref: 'subCategories' },
}, { timestamps: true });

// ====== Mongoose Models ======
const Bulk = mongoose.model('Bulk', bulkSchema);
const Category = mongoose.model('Category', categorySchema);
const subCategories = mongoose.model('subCategories', subCategorySchema);
const Product = mongoose.model('Product', productSchema);

// ====== CSV Writers ======
const successCsvWriter = format({ headers: true });
const failedCsvWriter = format({ headers: true });

const successStream = fs.createWriteStream('successful_products.csv');
const failedStream = fs.createWriteStream('failed_products.csv');
successCsvWriter.pipe(successStream);
failedCsvWriter.pipe(failedStream);

// ====== Lookup Caches ======
const bulkCache = new Map();
const categoryCache = new Map();
const subCategoryCache = new Map();

// ====== Helper: Get or cache ObjectId ======
const getCachedId = async (collection, cache, key, query) => {
  if (cache.has(key)) return cache.get(key);
  const doc = await collection.findOne(query);
  if (doc) cache.set(key, doc._id);
  return doc ? doc._id : null;
};

// ====== Main Upsert Logic ======
const insertOrUpdateProducts = async (productList) => {
  for (const data of productList) {
    const {
      sku,
      parentSku,
      bulk,
      variant_name,
      grossWeight,
      netWeight,
      itemQty,
      displayName,
      category,
      sub_category,
    } = data;

    try {
      const bulkId = await getCachedId(Bulk, bulkCache, bulk, { sku: bulk.trim() });
      const categoryId = await getCachedId(Category, categoryCache, category, { name: category.trim() });
      const subCategoryId = await getCachedId(subCategories, subCategoryCache, sub_category, { name: sub_category.trim() });

      const missing = [];
      if (!bulkId) missing.push('bulk');
      if (!categoryId) missing.push('category');
      if (!subCategoryId) missing.push('sub_category');

      if (missing.length > 0) {
        failedCsvWriter.write({ ...data, error: `Missing: ${missing.join(', ')}` });
        console.warn(`⚠️ Skipped SKU: ${sku} due to missing ${missing.join(', ')}`);
        continue;
      }

      const updateData = {
        parentSku,
        variant_name,
        grossWeight: parseFloat(grossWeight),
        netWeight: parseFloat(netWeight),
        itemQty: parseInt(itemQty),
        displayName,
        bulk: bulkId,
        category: categoryId,
        sub_category: subCategoryId,
      };

      await Product.findOneAndUpdate({ sku: sku.trim() }, { $set: updateData }, { upsert: true, new: true });
      successCsvWriter.write({ ...data, status: 'success' });
      console.log(`✔️ Processed SKU: ${sku}`);
    } catch (err) {
      failedCsvWriter.write({ ...data, error: err.message });
      console.error(`❌ Error processing SKU: ${sku}`, err);
    }
  }

  successCsvWriter.end();
  failedCsvWriter.end();
  console.log('🎉 Done processing all products.');
  mongoose.disconnect();
};

// ====== Load & Parse CSV ======
const productList = [];

fs.createReadStream('insertfinal.csv')
  .pipe(csv())
  .on('data', (row) => {
    productList.push({
      sku: row.sku,
      parentSku: row.parentSku,
      bulk: row.bulk,
      variant_name: row.variant_name,
      grossWeight: row.grossWeight,
      netWeight: row.netWeight,
      itemQty: row.itemQty,
      displayName: row.displayName,
      category: row.category,
      sub_category: row.sub_category,
    });
  })
  .on('end', () => {
    console.log(`📦 Loaded ${productList.length} products from CSV`);
    insertOrUpdateProducts(productList);
  });




  