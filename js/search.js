// When the user clicks on the search box, we want to toggle the search dropdown
function displayToggleSearch(e) {
  e.preventDefault();
  e.stopPropagation();

  closeDropdownSearch(e);
  
  if (idx === null) {
    console.log("Building search index...");
    prepareIdxAndDocMap();
    console.log("Search index built.");
  }
  const dropdown = document.querySelector("#search-dropdown-content");
  if (dropdown) {
    if (!dropdown.classList.contains("show")) {
      dropdown.classList.add("show");
    }
    document.addEventListener("click", closeDropdownSearch);
    document.addEventListener("keydown", searchOnKeyDown);
    document.addEventListener("keyup", searchOnKeyUp);
  }
}

//We want to prepare the index only after clicking the search bar
var idx = null
const docMap = new Map()

function prepareIdxAndDocMap() {
  const docs = [  
    {
      "title": "changelog",
      "url": "/kafka4s/extra_md/changelog.html",
      "content": "changelog"
    } ,    
    {
      "title": "code of conduct",
      "url": "/kafka4s/extra_md/code-of-conduct.html",
      "content": "Code of Conduct We are committed to providing a friendly, safe and welcoming environment for all, regardless of level of experience, gender, gender identity and expression, sexual orientation, disability, personal appearance, body size, race, ethnicity, age, religion, nationality, or other such characteristics. Everyone is expected to follow the Scala Code of Conduct when discussing the project on the available communication channels. If you are being harassed, please contact us immediately so that we can support you. Moderation Any questions, concerns, or moderation requests please contact a member of the project. Zach Cox"
    } ,    
    {
      "title": "Getting Started",
      "url": "/kafka4s/docs/",
      "content": "Getting dependency To use kafka4s in an existing SBT project with Scala 2.13 or a later version, add the following dependencies to your build.sbt depending on your needs: libraryDependencies ++= Seq( \"com.banno\" %% \"kafka4s\" % \"&lt;version&gt;\" ) Some quick examples First, some initial imports: import cats._, cats.effect._, cats.implicits._, scala.concurrent.duration._ Define our data We’ll define a toy message type for data we want to store in our Kafka topic. case class Customer(name: String, address: String) case class CustomerId(id: String) Create our Kafka topic Now we’ll tell Kafka to create a topic that we’ll write our Kafka records to. First, let’s bring some types and implicits into scope: import com.banno.kafka._, com.banno.kafka.admin._ import org.apache.kafka.clients.admin.NewTopic Now we can create a topic named customers.v1 with 1 partition and 1 replica: val topic = new NewTopic(\"customers.v1\", 1, 1.toShort) // topic: NewTopic = (name=customers.v1, numPartitions=1, replicationFactor=1, replicasAssignments=null, configs=null) val kafkaBootstrapServers = \"localhost:9092\" // Change as needed // kafkaBootstrapServers: String = \"localhost:9092\" import cats.effect.unsafe.implicits.global AdminApi.createTopicsIdempotent[IO](kafkaBootstrapServers, topic :: Nil).unsafeRunSync() Register our topic schema Let’s register a schema for our topic with the schema registry! First, we bring types and implicits into scope: import com.banno.kafka.schemaregistry._ We’ll use the name of the topic we created above: val topicName = topic.name // topicName: String = \"customers.v1\" Now we can register our topic key and topic value schemas: val schemaRegistryUri = \"http://localhost:8091\" // Change as needed // schemaRegistryUri: String = \"http://localhost:8091\" import cats.effect.unsafe.implicits.global SchemaRegistryApi.register[IO, CustomerId, Customer]( schemaRegistryUri, topicName ).unsafeRunSync() Write our records to Kafka Now let’s create a producer and send some records to our Kafka topic! We first bring our Kafka producer utils into scope: import com.banno.kafka.producer._ Now we can create our producer instance: val producer = ProducerApi.Avro.Generic.resource[IO]( BootstrapServers(kafkaBootstrapServers), SchemaRegistryUrl(schemaRegistryUri), ClientId(\"producer-example\") ) // producer: Resource[IO, ProducerApi[IO, org.apache.avro.generic.GenericRecord, org.apache.avro.generic.GenericRecord]] = Allocate( // resource = cats.effect.kernel.Resource$$$Lambda$7196/727064108@5ff3e321 // ) And we’ll define some customer records to be written: import org.apache.kafka.clients.producer.ProducerRecord val recordsToBeWritten = (1 to 10).map(a =&gt; new ProducerRecord(topicName, CustomerId(a.toString), Customer(s\"name-${a}\", s\"address-${a}\"))).toVector // recordsToBeWritten: Vector[ProducerRecord[CustomerId, Customer]] = Vector( // ProducerRecord(topic=customers.v1, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=CustomerId(1), value=Customer(name-1,address-1), timestamp=null), // ProducerRecord(topic=customers.v1, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=CustomerId(2), value=Customer(name-2,address-2), timestamp=null), // ProducerRecord(topic=customers.v1, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=CustomerId(3), value=Customer(name-3,address-3), timestamp=null), // ProducerRecord(topic=customers.v1, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=CustomerId(4), value=Customer(name-4,address-4), timestamp=null), // ProducerRecord(topic=customers.v1, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=CustomerId(5), value=Customer(name-5,address-5), timestamp=null), // ProducerRecord(topic=customers.v1, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=CustomerId(6), value=Customer(name-6,address-6), timestamp=null), // ProducerRecord(topic=customers.v1, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=CustomerId(7), value=Customer(name-7,address-7), timestamp=null), // ProducerRecord(topic=customers.v1, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=CustomerId(8), value=Customer(name-8,address-8), timestamp=null), // ProducerRecord(topic=customers.v1, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=CustomerId(9), value=Customer(name-9,address-9), timestamp=null), // ProducerRecord(topic=customers.v1, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=CustomerId(10), value=Customer(name-10,address-10), timestamp=null) // ) And now we can (attempt to) write our records to Kafka: producer.use(p =&gt; recordsToBeWritten.traverse_(p.sendSync)) // error: type mismatch; // ProducerRecord[GenericRecord|CustomerId, GenericRecord|Customer] =&gt; IO[RecordMetadata] // producer.use(p =&gt; recordsToBeWritten.traverse_(p.sendSync)) // ^^^^^^^^^^ The above fails to compile, however! Our producer writes generic ProducerRecords, but we’d like to send typed records, to ensure that our CustomerId key and our Customer value are compatible with our topic. For this, we can use Kafka4s’ avro4s integration! Writing typed records with an Avro4s producer Turning a generic producer into a typed producer is simple. We first ensure that com.sksamuel.avro4s.RecordFormat instances for our data are in scope: implicit val CustomerRecordFormat = com.sksamuel.avro4s.RecordFormat[Customer] // CustomerRecordFormat: com.sksamuel.avro4s.RecordFormat[Customer] = com.sksamuel.avro4s.RecordFormat$$anon$1@1d1b8006 implicit val CustomerIdRecordFormat = com.sksamuel.avro4s.RecordFormat[CustomerId] // CustomerIdRecordFormat: com.sksamuel.avro4s.RecordFormat[CustomerId] = com.sksamuel.avro4s.RecordFormat$$anon$1@f6c72b0 And with those implicits in scope, we can create our producer: val avro4sProducer = producer.map(_.toAvro4s[CustomerId, Customer]) // avro4sProducer: Resource[IO, ProducerApi[IO[A], CustomerId, Customer]] = Bind( // source = Allocate( // resource = cats.effect.kernel.Resource$$$Lambda$7196/727064108@5ff3e321 // ), // fs = cats.effect.kernel.Resource$$Lambda$7283/1973350410@3caff1de // ) We can now write our typed customer records successfully! import cats.effect.unsafe.implicits.global avro4sProducer.use(p =&gt; recordsToBeWritten.traverse_(r =&gt; p.sendSync(r).flatMap(rmd =&gt; IO(println(s\"Wrote record to ${rmd}\")))) ).unsafeRunSync() Read our records from Kafka Now that we’ve stored some records in Kafka, let’s read them as an fs2.Stream! We first import our Kafka consumer utilities: import com.banno.kafka.consumer._ Now we can create our consumer instance. By default, kafka4s consumers shift blocking calls to a dedicated ExecutionContext backed by a singleton thread pool, to avoid blocking the main work pool’s (typically ExecutionContext.global) threads, and as a simple synchronization mechanism because the underlying Java client KafkaConsumer is not thread-safe. After receiving records, work is then shifted back to the work pool. And here’s our consumer, which is using Avro4s to deserialize the records: val consumer = ConsumerApi.Avro4s.resource[IO, CustomerId, Customer]( BootstrapServers(kafkaBootstrapServers), SchemaRegistryUrl(schemaRegistryUri), ClientId(\"consumer-example\"), GroupId(\"consumer-example-group\") ) // consumer: Resource[IO, ConsumerApi[IO, CustomerId, Customer]] = Bind( // source = Bind( // source = Allocate( // resource = cats.effect.kernel.Resource$$$Lambda$7196/727064108@50baed09 // ), // fs = com.banno.kafka.consumer.ConsumerApi$Avro$$$Lambda$7286/546680304@5ea2310 // ), // fs = cats.effect.kernel.Resource$$Lambda$7283/1973350410@52661d8e // ) With our Kafka consumer in hand, we’ll assign to our consumer our topic partition, with no offsets, so that it starts reading from the first record, and read a stream of records from our Kafka topic: import org.apache.kafka.common.TopicPartition val initialOffsets = Map.empty[TopicPartition, Long] // Start from beginning // initialOffsets: Map[TopicPartition, Long] = Map() import cats.effect.unsafe.implicits.global val messages = consumer.use(c =&gt; c.assign(topicName, initialOffsets) *&gt; c.recordStream(1.second).take(5).compile.toVector ).unsafeRunSync() Because the producer and consumer above were created within a Resource context, everything was closed and shut down properly. Now that we’ve seen a quick overview, we can take a look at more in-depth documentation of Kafka4s utilities."
    } ,      
    {
      "title": "Kafka Client Metrics",
      "url": "/kafka4s/docs/kafka-client-metrics.html",
      "content": "Kafka Client Metrics Docs coming soon. Check out the kafka4s Scaladoc for more info."
    } ,    
    {
      "title": "Kafka Consumers",
      "url": "/kafka4s/docs/kafka-consumers.html",
      "content": "Using Kafka Consumers Docs coming soon. Check out the kafka4s Scaladoc for more info."
    } ,    
    {
      "title": "Kafka Producers",
      "url": "/kafka4s/docs/kafka-producers.html",
      "content": "Using Kafka Producers Docs coming soon. Check out the kafka4s Scaladoc for more info."
    } ,    
    {
      "title": "license",
      "url": "/kafka4s/extra_md/license.html",
      "content": "Apache License Version 2.0, January 2004 http://www.apache.org/licenses/ TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION Definitions. “License” shall mean the terms and conditions for use, reproduction, and distribution as defined by Sections 1 through 9 of this document. “Licensor” shall mean the copyright owner or entity authorized by the copyright owner that is granting the License. “Legal Entity” shall mean the union of the acting entity and all other entities that control, are controlled by, or are under common control with that entity. For the purposes of this definition, “control” means (i) the power, direct or indirect, to cause the direction or management of such entity, whether by contract or otherwise, or (ii) ownership of fifty percent (50%) or more of the outstanding shares, or (iii) beneficial ownership of such entity. “You” (or “Your”) shall mean an individual or Legal Entity exercising permissions granted by this License. “Source” form shall mean the preferred form for making modifications, including but not limited to software source code, documentation source, and configuration files. “Object” form shall mean any form resulting from mechanical transformation or translation of a Source form, including but not limited to compiled object code, generated documentation, and conversions to other media types. “Work” shall mean the work of authorship, whether in Source or Object form, made available under the License, as indicated by a copyright notice that is included in or attached to the work (an example is provided in the Appendix below). “Derivative Works” shall mean any work, whether in Source or Object form, that is based on (or derived from) the Work and for which the editorial revisions, annotations, elaborations, or other modifications represent, as a whole, an original work of authorship. For the purposes of this License, Derivative Works shall not include works that remain separable from, or merely link (or bind by name) to the interfaces of, the Work and Derivative Works thereof. “Contribution” shall mean any work of authorship, including the original version of the Work and any modifications or additions to that Work or Derivative Works thereof, that is intentionally submitted to Licensor for inclusion in the Work by the copyright owner or by an individual or Legal Entity authorized to submit on behalf of the copyright owner. For the purposes of this definition, “submitted” means any form of electronic, verbal, or written communication sent to the Licensor or its representatives, including but not limited to communication on electronic mailing lists, source code control systems, and issue tracking systems that are managed by, or on behalf of, the Licensor for the purpose of discussing and improving the Work, but excluding communication that is conspicuously marked or otherwise designated in writing by the copyright owner as “Not a Contribution.” “Contributor” shall mean Licensor and any individual or Legal Entity on behalf of whom a Contribution has been received by Licensor and subsequently incorporated within the Work. Grant of Copyright License. Subject to the terms and conditions of this License, each Contributor hereby grants to You a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable copyright license to reproduce, prepare Derivative Works of, publicly display, publicly perform, sublicense, and distribute the Work and such Derivative Works in Source or Object form. Grant of Patent License. Subject to the terms and conditions of this License, each Contributor hereby grants to You a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable (except as stated in this section) patent license to make, have made, use, offer to sell, sell, import, and otherwise transfer the Work, where such license applies only to those patent claims licensable by such Contributor that are necessarily infringed by their Contribution(s) alone or by combination of their Contribution(s) with the Work to which such Contribution(s) was submitted. If You institute patent litigation against any entity (including a cross-claim or counterclaim in a lawsuit) alleging that the Work or a Contribution incorporated within the Work constitutes direct or contributory patent infringement, then any patent licenses granted to You under this License for that Work shall terminate as of the date such litigation is filed. Redistribution. You may reproduce and distribute copies of the Work or Derivative Works thereof in any medium, with or without modifications, and in Source or Object form, provided that You meet the following conditions: (a) You must give any other recipients of the Work or Derivative Works a copy of this License; and (b) You must cause any modified files to carry prominent notices stating that You changed the files; and (c) You must retain, in the Source form of any Derivative Works that You distribute, all copyright, patent, trademark, and attribution notices from the Source form of the Work, excluding those notices that do not pertain to any part of the Derivative Works; and (d) If the Work includes a “NOTICE” text file as part of its distribution, then any Derivative Works that You distribute must include a readable copy of the attribution notices contained within such NOTICE file, excluding those notices that do not pertain to any part of the Derivative Works, in at least one of the following places: within a NOTICE text file distributed as part of the Derivative Works; within the Source form or documentation, if provided along with the Derivative Works; or, within a display generated by the Derivative Works, if and wherever such third-party notices normally appear. The contents of the NOTICE file are for informational purposes only and do not modify the License. You may add Your own attribution notices within Derivative Works that You distribute, alongside or as an addendum to the NOTICE text from the Work, provided that such additional attribution notices cannot be construed as modifying the License. You may add Your own copyright statement to Your modifications and may provide additional or different license terms and conditions for use, reproduction, or distribution of Your modifications, or for any such Derivative Works as a whole, provided Your use, reproduction, and distribution of the Work otherwise complies with the conditions stated in this License. Submission of Contributions. Unless You explicitly state otherwise, any Contribution intentionally submitted for inclusion in the Work by You to the Licensor shall be under the terms and conditions of this License, without any additional terms or conditions. Notwithstanding the above, nothing herein shall supersede or modify the terms of any separate license agreement you may have executed with Licensor regarding such Contributions. Trademarks. This License does not grant permission to use the trade names, trademarks, service marks, or product names of the Licensor, except as required for reasonable and customary use in describing the origin of the Work and reproducing the content of the NOTICE file. Disclaimer of Warranty. Unless required by applicable law or agreed to in writing, Licensor provides the Work (and each Contributor provides its Contributions) on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, including, without limitation, any warranties or conditions of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE. You are solely responsible for determining the appropriateness of using or redistributing the Work and assume any risks associated with Your exercise of permissions under this License. Limitation of Liability. In no event and under no legal theory, whether in tort (including negligence), contract, or otherwise, unless required by applicable law (such as deliberate and grossly negligent acts) or agreed to in writing, shall any Contributor be liable to You for damages, including any direct, indirect, special, incidental, or consequential damages of any character arising as a result of this License or out of the use or inability to use the Work (including but not limited to damages for loss of goodwill, work stoppage, computer failure or malfunction, or any and all other commercial damages or losses), even if such Contributor has been advised of the possibility of such damages. Accepting Warranty or Additional Liability. While redistributing the Work or Derivative Works thereof, You may choose to offer, and charge a fee for, acceptance of support, warranty, indemnity, or other liability obligations and/or rights consistent with this License. However, in accepting such obligations, You may act only on Your own behalf and on Your sole responsibility, not on behalf of any other Contributor, and only if You agree to indemnify, defend, and hold each Contributor harmless for any liability incurred by, or claims asserted against, such Contributor by reason of your accepting any such warranty or additional liability. END OF TERMS AND CONDITIONS APPENDIX: How to apply the Apache License to your work. To apply the Apache License to your work, attach the following boilerplate notice, with the fields enclosed by brackets \"[]\" replaced with your own identifying information. (Don't include the brackets!) The text should be enclosed in the appropriate comment syntax for the file format. We also recommend that a file or class name and description of purpose be included on the same \"printed page\" as the copyright notice for easier identification within third-party archives. Copyright [yyyy] [name of copyright owner] Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License."
    } ,      
    {
      "title": "Schema Registry Utils",
      "url": "/kafka4s/docs/schema-registry.html",
      "content": "Schema Registry Utils Docs coming soon. Check out the kafka4s Scaladoc for more info."
    } ,      
  ];

  idx = lunr(function () {
    this.ref("title");
    this.field("content");

    docs.forEach(function (doc) {
      this.add(doc);
    }, this);
  });

  docs.forEach(function (doc) {
    docMap.set(doc.title, doc.url);
  });
}

// The onkeypress handler for search functionality
function searchOnKeyDown(e) {
  const keyCode = e.keyCode;
  const parent = e.target.parentElement;
  const isSearchBar = e.target.id === "search-bar";
  const isSearchResult = parent ? parent.id.startsWith("result-") : false;
  const isSearchBarOrResult = isSearchBar || isSearchResult;

  if (keyCode === 40 && isSearchBarOrResult) {
    // On 'down', try to navigate down the search results
    e.preventDefault();
    e.stopPropagation();
    selectDown(e);
  } else if (keyCode === 38 && isSearchBarOrResult) {
    // On 'up', try to navigate up the search results
    e.preventDefault();
    e.stopPropagation();
    selectUp(e);
  } else if (keyCode === 27 && isSearchBarOrResult) {
    // On 'ESC', close the search dropdown
    e.preventDefault();
    e.stopPropagation();
    closeDropdownSearch(e);
  }
}

// Search is only done on key-up so that the search terms are properly propagated
function searchOnKeyUp(e) {
  // Filter out up, down, esc keys
  const keyCode = e.keyCode;
  const cannotBe = [40, 38, 27];
  const isSearchBar = e.target.id === "search-bar";
  const keyIsNotWrong = !cannotBe.includes(keyCode);
  if (isSearchBar && keyIsNotWrong) {
    // Try to run a search
    runSearch(e);
  }
}

// Move the cursor up the search list
function selectUp(e) {
  if (e.target.parentElement.id.startsWith("result-")) {
    const index = parseInt(e.target.parentElement.id.substring(7));
    if (!isNaN(index) && (index > 0)) {
      const nextIndexStr = "result-" + (index - 1);
      const querySel = "li[id$='" + nextIndexStr + "'";
      const nextResult = document.querySelector(querySel);
      if (nextResult) {
        nextResult.firstChild.focus();
      }
    }
  }
}

// Move the cursor down the search list
function selectDown(e) {
  if (e.target.id === "search-bar") {
    const firstResult = document.querySelector("li[id$='result-0']");
    if (firstResult) {
      firstResult.firstChild.focus();
    }
  } else if (e.target.parentElement.id.startsWith("result-")) {
    const index = parseInt(e.target.parentElement.id.substring(7));
    if (!isNaN(index)) {
      const nextIndexStr = "result-" + (index + 1);
      const querySel = "li[id$='" + nextIndexStr + "'";
      const nextResult = document.querySelector(querySel);
      if (nextResult) {
        nextResult.firstChild.focus();
      }
    }
  }
}

// Search for whatever the user has typed so far
function runSearch(e) {
  if (e.target.value === "") {
    // On empty string, remove all search results
    // Otherwise this may show all results as everything is a "match"
    applySearchResults([]);
  } else {
    const tokens = e.target.value.split(" ");
    const moddedTokens = tokens.map(function (token) {
      // "*" + token + "*"
      return token;
    })
    const searchTerm = moddedTokens.join(" ");
    const searchResults = idx.search(searchTerm);
    const mapResults = searchResults.map(function (result) {
      const resultUrl = docMap.get(result.ref);
      return { name: result.ref, url: resultUrl };
    })

    applySearchResults(mapResults);
  }

}

// After a search, modify the search dropdown to contain the search results
function applySearchResults(results) {
  const dropdown = document.querySelector("div[id$='search-dropdown'] > .dropdown-content.show");
  if (dropdown) {
    //Remove each child
    while (dropdown.firstChild) {
      dropdown.removeChild(dropdown.firstChild);
    }

    //Add each result as an element in the list
    results.forEach(function (result, i) {
      const elem = document.createElement("li");
      elem.setAttribute("class", "dropdown-item");
      elem.setAttribute("id", "result-" + i);

      const elemLink = document.createElement("a");
      elemLink.setAttribute("title", result.name);
      elemLink.setAttribute("href", result.url);
      elemLink.setAttribute("class", "dropdown-item-link");

      const elemLinkText = document.createElement("span");
      elemLinkText.setAttribute("class", "dropdown-item-link-text");
      elemLinkText.innerHTML = result.name;

      elemLink.appendChild(elemLinkText);
      elem.appendChild(elemLink);
      dropdown.appendChild(elem);
    });
  }
}

// Close the dropdown if the user clicks (only) outside of it
function closeDropdownSearch(e) {
  // Check if where we're clicking is the search dropdown
  if (e.target.id !== "search-bar") {
    const dropdown = document.querySelector("div[id$='search-dropdown'] > .dropdown-content.show");
    if (dropdown) {
      dropdown.classList.remove("show");
      document.documentElement.removeEventListener("click", closeDropdownSearch);
    }
  }
}
