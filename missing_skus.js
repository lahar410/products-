const data =[
  {
    "orderId": "#BHSample002",
    "sku": "BK-51",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "ORD/K0-97/20461_s1",
    "sku": "K-5041",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "ORD/K0-97/20461",
    "sku": "K-5041",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "ORD/K0-54/21266",
    "sku": "K-5041",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-54927",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "ORD/K0-57/23502",
    "sku": "K-5041",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-3993",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-5757",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-8797",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-8992",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-10761",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-11320",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-14999",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-16475",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-19893",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-23470",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-24597",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-25365",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-30789",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-31181",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-10292",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-10313",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KSK-33074",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "KO-39657",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "#BH1338576",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "#BH1341783",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "#BH1362411",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "#BH1365440",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "#BH1371649",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "#BH1388570",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "#BH1399051",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "#BH1422895",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "#BH1439835",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "1867956_433380",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "#BH1494428",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "#BH1478958",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  },
  {
    "orderId": "1878149_438031",
    "sku": "K-5561",
    "reason": "SKU not found in products"
  }
]


const uniqueSkus = [...new Set(data.map(item => item.sku))];

console.log("Unique SKUs:", uniqueSkus);
