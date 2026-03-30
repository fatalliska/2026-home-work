package company.vk.edu.distrib.compute;

import java.util.Set;
import java.util.stream.Stream;

import company.vk.edu.distrib.compute.artttnik.MyKVServiceFactory;
import company.vk.edu.distrib.compute.mandesero.KVServiceFactoryImpl;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.ParameterDeclarations;
import org.junit.platform.commons.util.ReflectionUtils;

public class KVServiceFactoryArgumentsProvider implements ArgumentsProvider {
    private final Set<Class<? extends KVServiceFactory>> factories = Set.of(
            KVServiceFactoryImpl.class,
            MyKVServiceFactory.class
    );

    @Override
    @NonNull
    public Stream<? extends Arguments> provideArguments(
        @NonNull ParameterDeclarations parameters,
        @NonNull ExtensionContext context
    ) {
        return factories.stream()
            .map(ReflectionUtils::newInstance)
            .map(Arguments::of);
    }
}
