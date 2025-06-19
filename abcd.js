const fs = require('fs');
const csv = require('csv-parser');
const { format } = require('@fast-csv/format');

const skuFilePath = 'sku.csv';
const abcFilePath = 'abc.csv';
const outputFilePath = 'common_skus_output2.csv';

const skuSet = new Set();
const matchedRows = [];

// Step 1: Read sku.csv and store SKUs in a Set
fs.createReadStream(skuFilePath)
  .pipe(csv())
  .on('data', (row) => {
    const sku = row['sku']?.trim();
    if (sku) {
      skuSet.add(sku);
    }
  })
  .on('end', () => {
    console.log('✅ SKU list loaded.');

    // Step 2: Read abc.csv and filter rows where SKU is in skuSet
    fs.createReadStream(abcFilePath)
      .pipe(csv())
      .on('data', (row) => {
        const sku = row['sku']?.trim();
        if (skuSet.has(sku)) {
          matchedRows.push(row);
        }
      })
      .on('end', () => {
        console.log(`✅ Found ${matchedRows.length} matching SKUs.`);

        // Step 3: Write matched rows to output CSV
        const outputStream = fs.createWriteStream(outputFilePath);
        const csvStream = format({ headers: true });

        csvStream.pipe(outputStream);
        matchedRows.forEach(row => csvStream.write(row));
        csvStream.end();

        console.log(`✅ Output written to ${outputFilePath}`);
      });
  });
