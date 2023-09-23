# Idea 自定义Maven ArcheType项目骨架

什么是ArcheType?

Archetype是一个Maven项目模板工具包。通过Archetype我们可以快速搭建Maven项目。通常我们使用idea创建maven工程的时候都会选择Archetype来创建项目，常用Archetype如下：

<img src="https://cdn.jsdelivr.net/gh/yiluohan1234/PicgoImg/img/202110261545710.png" alt="image-20211026154537027" style="zoom: 67%;" />

### 一、gitclone项目模版

```
git clone https://github.com/luojiahan/spark-quickstart-scala
```

![](https://cdn.jsdelivr.net/gh/yiluohan1234/PicgoImg/img/202110261533104.png)

### **二、生成archeType模版文件**

打开项目所在目录，运行命令：

```
mvn archetype:create-from-project
```

![](https://cdn.jsdelivr.net/gh/yiluohan1234/PicgoImg/img/202110261534370.png)

### 三、将模版项目安装到maven并重新生成骨架文件

进入到target/generated-sources/archetype目录下，运行 `mvn install`，将这个jar安装到本地仓库，当我们用archetype生成项目时，就需要选中我们安装的jar。如果需要共享到别它人，需要再执行`mvn deploy`，安装到中央仓库。

安装完成后可以在你设置的本地maven repo中查看到自己生成的archetype。

<img src="https://cdn.jsdelivr.net/gh/yiluohan1234/PicgoImg/img/202110261535879.png" style="zoom:67%;" />

在项目中执行`mvn archetype:crawl`, 执行以上命令后在本地仓库的根目录中会生成archetype-catalog.xml文件

![](https://cdn.jsdelivr.net/gh/yiluohan1234/PicgoImg/img/202110261536104.png)

安装**Maven Archetype Catalogs**插件，在插件设置中添加本地文件即可

<img src="https://cdn.jsdelivr.net/gh/yiluohan1234/PicgoImg/img/202110261536726.png" style="zoom:67%;" />
