package org.grails.plugins.elasticsearch.conversion.marshall

import org.codehaus.groovy.grails.commons.DomainClassArtefactHandler
import org.codehaus.groovy.grails.commons.GrailsDomainClass
import org.codehaus.groovy.grails.commons.GrailsDomainClassProperty
import org.grails.plugins.elasticsearch.mapping.SearchableClassMapping
import org.grails.plugins.elasticsearch.mapping.SearchableClassPropertyMapping

class DeepDomainClassMarshaller extends DefaultMarshaller {
    protected doMarshall(instance) {
        def domainClass = getDomainClass(instance)
        // don't use instance class directly, instead unwrap from javaassist
        def marshallResult = [id: instance.id, 'class': domainClass.clazz.name]
        SearchableClassMapping scm = elasticSearchContextHolder.getMappingContext(domainClass)
        if (!scm) {
            throw new IllegalStateException("Domain class ${domainClass} is not searchable.")
        }
        for (SearchableClassPropertyMapping prop in scm.propertiesMapping) {
            def propertyMapping = scm.getPropertyMapping(prop.propertyName)
            if (!propertyMapping) {
                continue
            }
            def propertyClassName = instance."${prop.propertyName}"?.class?.name
            def propertyClass = instance."${prop.propertyName}"?.class
            def propertyValue = instance."${prop.propertyName}"

            // Domain marshalling
            if (DomainClassArtefactHandler.isDomainClass(propertyClass)) {
                String searchablePropertyName = getSearchablePropertyName()
                if (propertyValue.class."$searchablePropertyName") {
                    // todo fixme - will throw exception when no searchable field.
                    marshallingContext.lastParentPropertyName = prop.propertyName
                    marshallResult += [(prop.propertyName): ([id: propertyValue.ident(), 'class': propertyClassName] + marshallingContext.delegateMarshalling(propertyValue, propertyMapping.maxDepth))]
                } else {
                    marshallResult += [(prop.propertyName): [id: propertyValue.ident(), 'class': propertyClassName]]
                }

                // Non-domain marshalling
            } else {
                marshallingContext.lastParentPropertyName = prop.propertyName
                def marshalledValue = marshallingContext.delegateMarshalling(propertyValue)
                // Ugly XContentBuilder bug: it only checks for EXACT class match with java.util.Date
                // (sometimes it appears to be java.sql.Timestamp for persistent objects)
                if (marshalledValue instanceof java.util.Date) {
                    marshalledValue = new java.util.Date(marshalledValue.getTime())
                }
                marshallResult += [(prop.propertyName): marshalledValue]
            }
        }
        marshallResult
    }

    protected nullValue() {
        return []
    }

    private GrailsDomainClass getDomainClass(instance) {
        def instanceClass = domainClassUnWrapperChain.unwrap(instance).class
        grailsApplication.domainClasses.find { it.clazz == instanceClass }
    }

    private String getSearchablePropertyName() {
        def searchablePropertyName = elasticSearchContextHolder.config.searchableProperty.name

        //Maintain backwards compatibility. Searchable property name may not be defined
        if (!searchablePropertyName) {
            return 'searchable'
        }
        searchablePropertyName
    }
}
