package site.ycsb.db;


//
//  {
//    "productScore": 0.06580772031001791,
//      "image": "https: //pigment.github.io/fake-logos/logos/medium/color/10.png",
//      "creator": "Raphael",
//      "code": "1591839259749",
//      "color": "teal",
//      "productName": "OF 12 COLOURED PENCILS ALICEBLUE PACK",
//      "inSale": "0",
//      "material": "Granite",
//      "price": "60.41",
//      "inStock": "1",
//      "shipsFrom": "nor",
//      "department": "Baby   Health",
//      "currencyCode": "UAH",
//      "brand": "Ryan Group",
//      "productDescription": "User-friendly discrete concept",
//      "stockCount": "163"
//  }

import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;

import java.util.HashMap;

/**
 * Ecommerce document class for RedisJSON YCSB.
 */
public class CommerceDocument {
  protected String productScore;
  protected String image;
  protected String creator;
  protected String code;
  protected String color;
  protected String productName;
  protected String inSale;
  protected String material;
  protected String price;
  protected String inStock;
  protected String shipsFrom;
  protected String department;
  protected String currencyCode;
  protected String brand;
  protected String productDescription;
  protected String stockCount;

  public HashMap<String, ByteIterator> getFieldMap() {
    HashMap<String, ByteIterator> values = new HashMap<>();
    values.put("productScore", new StringByteIterator(this.productScore));
    values.put("image", new StringByteIterator(this.image));
    values.put("creator", new StringByteIterator(this.creator));
    values.put("code", new StringByteIterator(this.code));
    values.put("color", new StringByteIterator(this.color));
    values.put("productName", new StringByteIterator(this.productName));
    values.put("inSale", new StringByteIterator(this.inSale));
    values.put("material", new StringByteIterator(this.material));
    values.put("price", new StringByteIterator(this.price));
    values.put("inStock", new StringByteIterator(this.inStock));
    values.put("shipsFrom", new StringByteIterator(this.shipsFrom));
    values.put("department", new StringByteIterator(this.department));
    values.put("currencyCode", new StringByteIterator(this.currencyCode));
    values.put("brand", new StringByteIterator(this.brand));
    values.put("productDescription", new StringByteIterator(this.productDescription));
    values.put("stockCount", new StringByteIterator(this.stockCount));
    return values;
  }

  @Override
  public String toString() {
    return "CommerceDocument{" +
        "productScore='" + productScore + '\'' +
        ", image='" + image + '\'' +
        ", creator='" + creator + '\'' +
        ", code='" + code + '\'' +
        ", color='" + color + '\'' +
        ", productName='" + productName + '\'' +
        ", inSale='" + inSale + '\'' +
        ", material='" + material + '\'' +
        ", price='" + price + '\'' +
        ", inStock='" + inStock + '\'' +
        ", shipsFrom='" + shipsFrom + '\'' +
        ", department='" + department + '\'' +
        ", currencyCode='" + currencyCode + '\'' +
        ", brand='" + brand + '\'' +
        ", productDescription='" + productDescription + '\'' +
        ", stockCount='" + stockCount + '\'' +
        '}';
  }
}
