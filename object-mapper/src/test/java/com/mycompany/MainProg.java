package com.mycompany;

import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import me.prettyprint.hom.CassandraTestBase;
import me.prettyprint.hom.Colors;

public class MainProg {

  public static void main(String[] args) {
    // TODO: Create keyspace "TestKeyspace" and column family "TestColumnFamily" before running test
    EntityManagerFactory emf = Persistence.createEntityManagerFactory("cassandraPersistenceUnit");
    EntityManager em = emf.createEntityManager();
    try {
      MyPojo pojo1 = new MyPojo();
      pojo1.setId(UUID.randomUUID());
      pojo1.setLongProp1(123L);
      pojo1.setColor(Colors.RED);

      em.persist(pojo1);

      // do some stuff

      MyPojo pojo2 = em.find(MyPojo.class, pojo1.getId());

      // do some more stuff

      System.out.println("Color = " + pojo2.getColor());
    } finally {
      emf.close();
    }
  }
}