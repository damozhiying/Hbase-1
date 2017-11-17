package com.impetus.kundera;

import com.impetus.kundera.entities.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.junit.*;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * The Class CRUDTest.
 */
public class CRUDTest {

    private static Logger logger = Logger.getLogger( CRUDTest.class );

    //hadoop的配置对象
    private static Configuration configuration;

    //PU常量
    private static final String PU = "hbase_pu";

    //实体管理工厂对象
    private static EntityManagerFactory emf;

    //实体管理对象
    private EntityManager em;

    //zookeeper集群host
    private static final String ZOOKEEPER_HOST = "zookeeper1,zookeeper2,zookeeper3";

    //zookeeper集群端口
    private static final String ZOOKEEPER_PORT = "2181";

    /**
     * Sets the up before class.
     *
     * @throws Exception the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass () throws Exception {

        configuration = HBaseConfiguration.create();
        configuration.set( "hbase.zookeeper.quorum", ZOOKEEPER_HOST );
        configuration.set( "hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT );
        emf = Persistence.createEntityManagerFactory( PU );
    }

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp () throws Exception {

        em = emf.createEntityManager();
    }

    /**
     * Test insert.
     *
     * @throws Exception the exception
     */
    @Test
    public void testInsert () throws Exception {

        Person p = new Person();
        p.setPersonId( "109" );
        p.setPersonName( "lisi" );
        p.setAge( 24 );
        em.persist( p );
        logger.info( "=================添加成功===============" );
    }

    @Test
    public void testFind () {

        Person person = em.find( Person.class, "109" );
        logger.info( "【" + person.toString() + "】");
        /*Assert.assertNotNull( person );
        Assert.assertEquals( "101", person.getPersonId() );
        Assert.assertEquals( "zhangsan", person.getPersonName() );*/
    }

    /**
     * Test merge.
     */
    private void testMerge () {

        Person person = em.find( Person.class, "101" );
        person.setPersonName( "devender" );
        em.merge( person );

        Person p1 = em.find( Person.class, "101" );
        Assert.assertEquals( "devender", p1.getPersonName() );
    }

    /**
     * Test remove.
     */
    private void testRemove () {

        Person p = em.find( Person.class, "101" );
        em.remove( p );

        Person p1 = em.find( Person.class, "101" );
        Assert.assertNull( p1 );
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @After
    public void tearDown () throws Exception {

        em.close();
    }

    /**
     * Tear down after class.
     *
     * @throws Exception the exception
     */
    @AfterClass
    public static void tearDownAfterClass () throws Exception {

        if ( emf != null ) {
            emf.close();
            emf = null;
        }
    }
}
