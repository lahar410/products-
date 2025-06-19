const mongoose = require('mongoose');
const fs = require('fs');
const csv = require('csv-parser');

// ====== MongoDB Connection ======
const MONGO_URI = 'mongodb+srv://CRM-Database-ReadOnly:1cS8EhFkOzKwlVuI@cluster0.okk1w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0&readPreference=secondaryPreferred';
const DB_NAME = 'CRM-Database';

// ====== Connect to MongoDB ======
mongoose.connect(MONGO_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  dbName: DB_NAME,
});

const db = mongoose.connection;
db.on('error', console.error.bind(console, 'MongoDB connection error:'));
db.once('open', async () => {
  console.log('✅ MongoDB connected');

  const Product = mongoose.model('Product', new mongoose.Schema({}, { strict: false }));

  const csvFilePath = 'finalproduct.csv';
  const csvData = [];

  fs.createReadStream(csvFilePath)
    .pipe(csv())
    .on('data', (row) => {
      csvData.push(row);
    })
    .on('end', async () => {
      console.log(`📦 Total CSV SKUs: ${csvData.length}`);

      const skusFromCSV = csvData.map((item) => item.sku);
      const existingProducts = await Product.find({ sku: { $in: skusFromCSV } }, { sku: 1 });
      const existingSkus = new Set(existingProducts.map((p) => p.sku));

      const notInDB = csvData.filter((item) => !existingSkus.has(item.sku));

      console.log(`🚫 SKUs not found in MongoDB: ${notInDB.length}`);

      // Save to CSV
      const outputPath = 'missingProducts2.csv';
      const header = Object.keys(csvData[0]).join(',') + '\n';
      const rows = notInDB.map((item) => Object.values(item).join(',')).join('\n');

      fs.writeFileSync(outputPath, header + rows, 'utf8');
      console.log(`✅ Missing products saved to ${outputPath}`);

      process.exit(0);
    });
});
